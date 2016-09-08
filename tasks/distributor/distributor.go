package distributor

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/mrd0ll4r/pyme"
	"github.com/mrd0ll4r/pyme/tasks"
)

type nodeRating struct {
	nodeID pyme.NodeID
	rating float64
}

type ratedTask struct {
	task    tasks.Task
	ratings []nodeRating
}

type extendedQueueStatistics struct {
	tasksPosted    int64
	tasksRunning   int64
	tasksCompleted int64
	tasksFailed    int64
	totalCost      float64
	lastAction     time.Time
	sync.RWMutex
}

type queueStatisticsContainer struct {
	queues map[string]*extendedQueueStatistics
	sync.RWMutex
}

type distributorLogic struct {
	id pyme.NodeID

	nodes     map[pyme.NodeID]*node
	nodesLock sync.RWMutex

	inactiveNodes     map[pyme.NodeID]*node
	inactiveNodesLock sync.RWMutex

	tasksToRate   chan tasks.Task
	incomingTasks chan tasks.Task

	ratedTasksToDistribute chan ratedTask

	queueStatistics queueStatisticsContainer

	failCounter     map[tasks.TaskID]int
	failCounterLock sync.RWMutex

	cfg Config

	sync.RWMutex
}

// Config is the configuration of a Distributor.
type Config struct {
	// MaxAllowedFails defines the maximum amount of times a task is allowed
	// to be retried after a failure.
	MaxAllowedFails int `yaml:"max_allowed_fails"`

	// NodeTimeout is the duration after which a node will be assumed to be
	// defunct if it has not announced.
	NodeTimeout time.Duration `yaml:"node_timeout"`

	// MaxParallelRateRequests is the maximum number of HTTP requests that
	// can be used for rating requests simultaneously per node.
	MaxParallelRateRequests int `yaml:"max_parallel_rate_requests"`

	// RatingBufferSize is the size of the buffer for ratings to send out.
	RatingBufferSize int `yaml:"rating_buffer_size"`

	// RatingBufferFlushInterval is the duration after which a rating buffer
	// will be flushed to a node if no new rating requests arrive.
	RatingBufferFlushInterval time.Duration `yaml:"rating_buffer_flush_interval"`

	// QueueStatsTTL defines the duration for which queue statistics are
	// kept.
	QueueStatsTTL time.Duration `yaml:"queue_stats_ttl"`

	// QueueStatsGCInterval is the interval between collecting old queue
	// statistics.
	QueueStatsGCInterval time.Duration `yaml:"queue_stats_gc_interval"`

	// RatingTimeout is the maximum amount of time to wait for a rating.
	// Note that ratings are sent out to all nodes in parallel, so we
	// collect all ratings we can get in this amount of time.
	RatingTimeout time.Duration `yaml:"rate_request_timeout"`

	// HTTPClientTimeout is the request timeout for the HTTP client
	// used to make rating requests.
	// Make sure this is higher than RatingTimeout.
	HTTPClientTimeout time.Duration `yaml:"http_client_timeout"`

	// DefaultNumWant is the default chunk size for handing out tasks
	// to a node if no value was given in the request or the value was
	// invalid.
	DefaultNumWant int `yaml:"default_num_want"`

	// Debug determines whether or not debug information about known nodes
	// will be printed.
	Debug bool
}

// NewDistributorLogic creates a new TaskDistributor.
func NewDistributorLogic(id pyme.NodeID, cfg Config) tasks.TaskDistributor {
	toReturn := &distributorLogic{
		id:                     id,
		nodes:                  make(map[pyme.NodeID]*node),
		inactiveNodes:          make(map[pyme.NodeID]*node),
		tasksToRate:            make(chan tasks.Task),
		incomingTasks:          make(chan tasks.Task),
		ratedTasksToDistribute: make(chan ratedTask),
		failCounter:            make(map[tasks.TaskID]int),
		queueStatistics: queueStatisticsContainer{
			queues: make(map[string]*extendedQueueStatistics),
		},
		cfg: cfg,
	}

	// Each of these receives a value synchronously from
	// ratedTasksToDistribute.
	// We could have more of them running, if necessary.
	for i := 0; i < cfg.MaxParallelRateRequests*cfg.RatingBufferSize; i++ {
		go toReturn.assignLoop()
	}

	// Each of these rates a single task.
	// we need nodeRatingBufferLen of them to actually fill the
	// rating buffer of a node and send a packet of tasks to rate.
	for i := 0; i < cfg.MaxParallelRateRequests*cfg.RatingBufferSize; i++ {
		go toReturn.ratingLoop()
	}

	go toReturn.incomingTasksBufferLoop()

	go func() {
		for {
			time.Sleep(cfg.QueueStatsGCInterval)
			cutoff := time.Now().Add(-cfg.QueueStatsTTL)
			toReturn.queueStatistics.Lock()
			for queue, stats := range toReturn.queueStatistics.queues {
				stats.RLock()
				if stats.lastAction.Before(cutoff) {
					delete(toReturn.queueStatistics.queues, queue)
				}
				stats.RUnlock()
			}
			toReturn.queueStatistics.Unlock()
		}
	}()

	if cfg.Debug {
		go toReturn.nodeStatisticsDisplayLoop()
	}

	return toReturn
}

func (t *distributorLogic) nodeStatisticsDisplayLoop() {
	oldValues := make(map[pyme.NodeID]tasks.NodeStatistics)
	for {
		time.Sleep(2 * time.Second)

		t.nodesLock.RLock()
		for _, node := range t.nodes {
			node.RLock()
			k := node.id
			v := node.stats
			node.RUnlock()
			if oldV, ok := oldValues[k]; ok {
				acceptedDiff := v.TasksAccepted - oldV.TasksAccepted
				completedDiff := v.TasksCompleted - oldV.TasksCompleted
				failedDiff := v.TasksFailed - oldV.TasksFailed
				notExecutedDiff := v.TasksNotExecuted - oldV.TasksNotExecuted

				acceptedPerSecond := float64(acceptedDiff) / float64(2)
				completedPerSecond := float64(completedDiff) / float64(2)
				failedPerSecond := float64(failedDiff) / float64(2)
				notExecutedPerSecond := float64(notExecutedDiff) / float64(2)

				fmt.Printf("Node %s: Accepted: %8d, Completed: %8d, Failed: %8d, NotExecuted: %8d (%5.2f accepts/s, %5.2f finishes/s, %5.2f fails/s, %5.2f notExecuted/s)\n",
					k, v.TasksAccepted, v.TasksCompleted, v.TasksFailed, v.TasksNotExecuted,
					acceptedPerSecond, completedPerSecond, failedPerSecond, notExecutedPerSecond)
			} else {
				fmt.Printf("Node %s: Accepted: %8d, Completed: %8d, Failed: %8d, NotExecuted: %8d\n", k, v.TasksAccepted, v.TasksCompleted, v.TasksFailed, v.TasksNotExecuted)
			}
			oldValues[k] = v
		}
		t.nodesLock.RUnlock()

		t.inactiveNodesLock.RLock()
		for _, node := range t.inactiveNodes {
			node.RLock()
			k := node.id
			v := node.stats
			node.RUnlock()
			fmt.Printf("(inactive) Node %s: Accepted: %8d, Completed: %8d, Failed: %8d, NotExecuted: %8d\n", k, v.TasksAccepted, v.TasksCompleted, v.TasksFailed, v.TasksNotExecuted)
		}
		t.inactiveNodesLock.RUnlock()
	}
}

func (t *distributorLogic) QueueStatistics() map[string]tasks.QueueStatistics {
	toReturn := make(map[string]tasks.QueueStatistics, 0)

	t.queueStatistics.RLock()
	defer t.queueStatistics.RUnlock()
	for queue, stats := range t.queueStatistics.queues {
		stats.RLock()

		qStats := tasks.QueueStatistics{
			TasksPosted:          uint64(stats.tasksPosted),
			TasksRunning:         uint64(stats.tasksRunning),
			TasksCompleted:       uint64(stats.tasksCompleted),
			TasksFailed:          uint64(stats.tasksFailed),
			AverageExecutionCost: stats.totalCost / float64((stats.tasksRunning + stats.tasksCompleted)),
		}
		if math.IsNaN(qStats.AverageExecutionCost) {
			qStats.AverageExecutionCost = -1
		}

		toReturn[queue] = qStats
		stats.RUnlock()
	}

	return toReturn
}

func (t *distributorLogic) updateQueueStatistics(queue string, deltaCost float64, deltaPosted, deltaRunning, deltaCompleted, deltaFailed int64) {
	t.queueStatistics.RLock()
	stats, ok := t.queueStatistics.queues[queue]
	t.queueStatistics.RUnlock()
	if !ok {
		stats = &extendedQueueStatistics{}
		t.queueStatistics.Lock()
		if _, ok = t.queueStatistics.queues[queue]; !ok {
			t.queueStatistics.queues[queue] = stats
		} else {
			stats = t.queueStatistics.queues[queue]
		}
		t.queueStatistics.Unlock()
	}

	stats.Lock()
	defer stats.Unlock()
	stats.tasksPosted += deltaPosted
	stats.tasksRunning += deltaRunning
	stats.tasksCompleted += deltaCompleted
	stats.tasksFailed += deltaFailed
	stats.totalCost += deltaCost

	stats.lastAction = time.Now()
}

func (t *distributorLogic) ratingLoop() {
	for {
		task := <-t.tasksToRate

		ratings := t.rateSingleTask(task)

		t.ratedTasksToDistribute <- ratedTask{task: task, ratings: ratings}
	}
}

func (t *distributorLogic) rateSingleTask(task tasks.Task) []nodeRating {
retry:
	t.nodesLock.RLock()
	for len(t.nodes) == 0 {
		t.nodesLock.RUnlock()
		time.Sleep(time.Second)
		t.nodesLock.RLock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.RatingTimeout)
	defer cancel()

	rChans := make(map[pyme.NodeID]<-chan float64)
	for id, node := range t.nodes {
		rChans[id] = node.rate(ctx, task)
	}
	t.nodesLock.RUnlock()

	rr := make(chan nodeRating, len(rChans))
	wg := sync.WaitGroup{}

	for id, c := range rChans {
		wg.Add(1)
		go func(id pyme.NodeID, c <-chan float64) {
			defer wg.Done()
			select {
			case cost := <-c:
				rr <- nodeRating{nodeID: id, rating: cost}
			case <-ctx.Done():
				// rating timeout
			}
		}(id, c)
	}

	// wait for results or timeout
	wg.Wait()

	// collect results
	nodeRatings := make([]nodeRating, 0, len(t.nodes))
	available := true
	for available {
		select {
		case rating := <-rr:
			nodeRatings = append(nodeRatings, rating)
		default:
			available = false
		}
	}

	if len(nodeRatings) == 0 {
		log.Printf("have no ratings for task %s, retryting...", task.ID)
		goto retry
	}

	return nodeRatings
}

func (t *distributorLogic) assignLoop() {
	for {
		ratedTask := <-t.ratedTasksToDistribute

		bestValue := math.MaxFloat64
		var bestNodes []pyme.NodeID
		// TODO add negative cost case
		for _, r := range ratedTask.ratings {
			if r.rating < bestValue {
				bestValue = r.rating
				bestNodes = []pyme.NodeID{r.nodeID}
			} else if r.rating == bestValue {
				bestNodes = append(bestNodes, r.nodeID)
			}
		}

		t.updateQueueStatistics(ratedTask.task.ID.Queue(), bestValue, 0, 1, 0, 0)

		if len(bestNodes) == 0 {
			log.Println("assignment got a task with zero ratings, taskID:", ratedTask.task.ID)
			continue
		}

		bestNode := bestNodes[0]
		// if we have more than one: chose randomly
		if len(bestNodes) > 1 {
			bestNode = bestNodes[rand.Intn(len(bestNodes))]
		}

		t.nodesLock.RLock()
		node, ok := t.nodes[bestNode]

		if !ok {
			t.nodesLock.RUnlock()
			// redistribute
			log.Printf("best node for task %s does not exist anymore, redistributing one task", ratedTask.task.ID)

			// has to be re-rated with the missing node out of the picture.
			t.incomingTasks <- ratedTask.task
			continue
		}

		err := node.enqueue(ratedTask.task)
		t.nodesLock.RUnlock()
		if err != nil {
			log.Printf("enqueue for task %s on node %s failed with %q, redistributing one task", ratedTask.task.ID, bestNode, err.Error())

			// has to be re-rated with the possibly inactive node out of the picture
			t.incomingTasks <- ratedTask.task
		}
	}
}

func (t *distributorLogic) HandIn(nodeID pyme.NodeID, taskID tasks.TaskID, status tasks.ExecutionStatus) error {
	t.nodesLock.RLock()
	node, ok := t.nodes[nodeID]
	t.nodesLock.RUnlock()

	if !ok {
		return errors.New("unknown node")
	}

	task, err := node.handIn(taskID, status)
	if err != nil {
		return errors.Wrap(err, "node rejected handin")
	}

	switch status {
	case tasks.Success:
		// do nothing - the node cleaned up the task
		t.updateQueueStatistics(taskID.Queue(), 0, 0, -1, 1, 0)
		t.removeFromFailCounterIfPresent(taskID)
		return nil
	case tasks.Failure:
		fails := t.insertOrIncrementFailCounter(taskID)
		if fails < t.cfg.MaxAllowedFails {
			t.updateQueueStatistics(taskID.Queue(), 0, 0, -1, 0, 0)
		} else {
			t.updateQueueStatistics(taskID.Queue(), 0, 0, -1, 0, 1)
			t.removeFromFailCounterIfPresent(taskID)
			log.Printf("Task %s failed %d times - abandoning", taskID, fails)
			return nil
		}
	case tasks.NotExecuted:
		t.updateQueueStatistics(taskID.Queue(), 0, 0, -1, 0, 0)
	default:
	}
	// redistribute
	t.incomingTasks <- task

	return nil
}

func (t *distributorLogic) insertOrIncrementFailCounter(taskID tasks.TaskID) int {
	t.failCounterLock.Lock()
	defer t.failCounterLock.Unlock()

	if val, ok := t.failCounter[taskID]; ok {
		t.failCounter[taskID] = val + 1
		return val + 1
	}
	t.failCounter[taskID] = 1
	return 1
}

func (t *distributorLogic) removeFromFailCounterIfPresent(taskID tasks.TaskID) {
	// try to avoid Locking the counter, use double-check idiom
	t.failCounterLock.RLock()
	if _, ok := t.failCounter[taskID]; !ok {
		t.failCounterLock.RUnlock()
		return
	}
	t.failCounterLock.RUnlock()
	t.failCounterLock.Lock()
	delete(t.failCounter, taskID)
	t.failCounterLock.Unlock()
}

func (t *distributorLogic) GetTasks(nodeID pyme.NodeID, numWant int, timeout time.Duration) ([]tasks.Task, error) {
	t.nodesLock.RLock()
	node, ok := t.nodes[nodeID]
	t.nodesLock.RUnlock()

	if !ok {
		return nil, errors.New("unknown node")
	}

	return node.getTasks(numWant, timeout)
}

func (t *distributorLogic) Announce(nodeID pyme.NodeID, ip net.IP, port uint16) (tasks.AnnounceResponse, error) {
	t.nodesLock.Lock()
	defer t.nodesLock.Unlock()
	node, ok := t.nodes[nodeID]

	if !ok {
		// let's see if it was inactive
		t.inactiveNodesLock.Lock()
		defer t.inactiveNodesLock.Unlock()

		node, ok = t.inactiveNodes[nodeID]
		if ok {
			// yes, it was just temporarily dead, let's revive it
			node.activate()
			delete(t.inactiveNodes, nodeID)
		} else {
			// nope, create a new node
			node = newNode(nodeID, ip, port, t.cfg)
		}
		t.nodes[nodeID] = node

		// start monitoring the node
		go func() {
			<-node.timeout()
			log.Printf("node %s timed out, deannouncing...", nodeID)
			err := t.deannounce(nodeID)
			if err != nil {
				log.Println(errors.Wrap(err, fmt.Sprintf("unable to deannounce node %s", nodeID)))
			}
		}()
	}

	node.updateEndpoint(ip, port)
	node.announce <- time.Now()
	return tasks.AnnounceResponse{NodeID: t.id}, nil
}

func (t *distributorLogic) deannounce(nodeID pyme.NodeID) error {
	t.nodesLock.Lock()
	defer t.nodesLock.Unlock()

	node, ok := t.nodes[nodeID]
	if !ok {
		// node already gone for some reason?
		return errors.New("missing node")
	}

	// redistribute work
	tasks := node.deactivate()
	go func() {
		if len(tasks) == 0 {
			return
		}
		log.Printf("redistributing %d tasks from node %s", len(tasks), nodeID)
		for _, task := range tasks {
			t.updateQueueStatistics(task.ID.Queue(), 0, 0, -1, 0, 0)
			t.incomingTasks <- task
		}
	}()

	t.inactiveNodesLock.Lock()
	defer t.inactiveNodesLock.Unlock()

	// add to inactive set
	t.inactiveNodes[nodeID] = node

	// remove from our nodes list
	delete(t.nodes, nodeID)

	return nil
}

func (t *distributorLogic) PostTask(queue string, task tasks.Task) error {
	// prepend our ID and the queue name
	task.ID = tasks.TaskID(fmt.Sprintf("%s-%s-%s", t.id, queue, task.ID))

	// put it in the queue to be buffered
	t.incomingTasks <- task

	t.updateQueueStatistics(queue, 0, 1, 0, 0, 0)
	return nil
}

func (t *distributorLogic) incomingTasksBufferLoop() {
	buffer := make([]tasks.Task, 0, 1000)

	for {
		// fill our buffer
		if len(buffer) == 0 {
			task := <-t.incomingTasks
			buffer = append(buffer, task)
		}

		select {
		case task := <-t.incomingTasks:
			buffer = append(buffer, task)
		case t.tasksToRate <- buffer[0]:
			buffer = buffer[1:]
		}
	}
}
