package nodeserver

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/mrd0ll4r/pyme"
	"github.com/mrd0ll4r/pyme/tasks"
)

// ErrUnknownTask is returned if a HandIn of an unknown (not marked as running)
// task is attempted.
var ErrUnknownTask = errors.New("unknown task")

// ErrAlreadyRunning is returned if it is attempted to mark a task as running
// that is already marked as such.
var ErrAlreadyRunning = errors.New("task already running")

// ErrNoMatchingCalculator is returned if it is attempted to calculate the cost
// for a task with a type for which there is no known Calculator.
var ErrNoMatchingCalculator = errors.New("no matching calculator")

type handInEntry struct {
	taskID tasks.TaskID
	status tasks.ExecutionStatus
}

type timestampedTask struct {
	tasks.Task
	timestamp time.Time
}

type taskContainer struct {
	tasks     []tasks.Task
	timestamp time.Time
	sync.Mutex
}

type nodeServerLogic struct {
	id pyme.NodeID

	distributors map[pyme.NodeID]tasks.Endpoint
	c            *http.Client

	runningTasks     map[tasks.TaskID]timestampedTask
	runningTasksLock sync.RWMutex

	handInsToReport chan handInEntry

	tasksToExecuteLocally     map[tasks.WorkerID]*taskContainer
	tasksToExecuteLocallyLock sync.RWMutex

	shutdown chan struct{}

	cfg Config

	calculators map[tasks.TaskType]tasks.Calculator

	sync.RWMutex
}

// Config models the configuration of a NodeServer.
type Config struct {
	// GCInterval is the interval to sleep between GC runs.
	GCInterval time.Duration `yaml:"gc_interval"`

	// GCCutoff is the maximum age for a task before it gets collected
	// by the next GC run.
	GCCutoff time.Duration `yaml:"gc_cutoff"`

	// AnnounceInterval is the interval to sleep between announcing to all
	// known distributors.
	AnnounceInterval time.Duration `yaml:"announce_interval"`

	// HandInBufferSize is the size of the buffer for hand-ins.
	HandInBufferSize int `yaml:"hand_in_buffer_size"`

	// HandInFlushInterval is the interval at which to flush the hand-in
	// buffer if no new hand-in was received during that time.
	HandInFlushInterval time.Duration `yaml:"hand_in_buffer_flush_interval"`

	// NumWantTasks is the amount of tasks to request from a distributor.
	NumWantTasks int `yaml:"num_want_tasks"`

	// GetTasksLongPollingTimeout is the timeout to specify when
	// long-polling for tasks at a distributor.
	GetTaskLongPollingTimeout time.Duration `yaml:"get_tasks_long_polling_timeout"`

	// Debug specifies whether or not to write debug logs.
	Debug bool `yaml:"debug"`
}

// NewNodeServerLogic creates a new NodeServer.
// It tries to announce to all given distributors once.
// If no distributor could be reached an error is returned.
func NewNodeServerLogic(id pyme.NodeID, calculators []tasks.Calculator, distributors []tasks.Endpoint, localEndpoint tasks.Endpoint, cfg Config) (tasks.NodeServer, error) {
	toReturn := &nodeServerLogic{
		id:                    id,
		distributors:          make(map[pyme.NodeID]tasks.Endpoint),
		c:                     &http.Client{},
		runningTasks:          make(map[tasks.TaskID]timestampedTask),
		handInsToReport:       make(chan handInEntry),
		tasksToExecuteLocally: make(map[tasks.WorkerID]*taskContainer),
		shutdown:              make(chan struct{}),
		calculators:           make(map[tasks.TaskType]tasks.Calculator),
		cfg:                   cfg,
	}

	for _, calc := range calculators {
		toReturn.calculators[calc.TaskType()] = calc
	}

	for _, ep := range distributors {
		resp, err := toReturn.announce(ep, localEndpoint)
		if err != nil {
			log.Println("unable to announce:", err)
			continue
		}
		toReturn.distributors[resp.NodeID] = ep
	}

	if len(toReturn.distributors) == 0 {
		return nil, errors.New("unable to perform initial announce to any of the distributors")
	}

	go toReturn.announceLoop(distributors, localEndpoint)

	go toReturn.handInLoop()

	go func() {
		for {
			time.Sleep(cfg.GCInterval)
			toReturn.collectGarbage(time.Now().Add(-cfg.GCCutoff))
		}
	}()

	if cfg.Debug {
		go func() {
			for {
				time.Sleep(2 * time.Second)
				toReturn.runningTasksLock.RLock()
				fmt.Printf("have %d tasks marked as currently running locally\n", len(toReturn.runningTasks))
				toReturn.runningTasksLock.RUnlock()
				toReturn.tasksToExecuteLocallyLock.RLock()
				fmt.Printf("tracking %d local worders:\n", len(toReturn.tasksToExecuteLocally))
				for worker, container := range toReturn.tasksToExecuteLocally {
					container.Lock()
					fmt.Printf("\thave %3d tasks waiting to be executed by worker %20s\n", len(container.tasks), string(worker))
					container.Unlock()
				}
				toReturn.tasksToExecuteLocallyLock.RUnlock()
			}
		}()
	}

	return toReturn, nil
}

func (t *nodeServerLogic) collectGarbage(cutoff time.Time) {
	log.Printf("running GC, collecting tasks given out before %s", cutoff.String())
	collected := 0

	t.runningTasksLock.Lock()
	for id, task := range t.runningTasks {
		if task.timestamp.Before(cutoff) {
			delete(t.runningTasks, id)
			t.handInsToReport <- handInEntry{taskID: id, status: tasks.NotExecuted}
			collected++
		}
	}
	t.runningTasksLock.Unlock()

	log.Printf("collecting stale worker queues that were not active after %s", cutoff.String())

	t.tasksToExecuteLocallyLock.Lock()
	for _, container := range t.tasksToExecuteLocally {
		container.Lock()
		if container.timestamp.Before(cutoff) {
			for _, task := range container.tasks {
				t.handInsToReport <- handInEntry{taskID: task.ID, status: tasks.NotExecuted}
				collected++
			}
			container.tasks = container.tasks[:0]
		}
		container.Unlock()
	}
	t.tasksToExecuteLocallyLock.Unlock()

	log.Printf("GC finished, handed in %d tasks as not executed", collected)
}

func (t *nodeServerLogic) Stop() {
	select {
	case <-t.shutdown:
		return
	default:
	}
	close(t.shutdown)

	// TODO report currently running tasks as not executed?
}

func (t *nodeServerLogic) RateTask(task tasks.Task) (tasks.TaskRating, error) {
	calc, ok := t.calculators[tasks.TaskType(strings.ToLower(string(task.Type)))]
	if !ok {
		return tasks.TaskRating{}, ErrNoMatchingCalculator
	}

	cost, err := calc.Calculate(task)
	if err != nil {
		return tasks.TaskRating{}, err
	}

	return tasks.TaskRating{ID: task.ID, Cost: cost}, nil
}

func (t *nodeServerLogic) announceLoop(distributors []tasks.Endpoint, localEndpoint tasks.Endpoint) {
	for {
		time.Sleep(t.cfg.AnnounceInterval)
		select {
		case <-t.shutdown:
			return
		default:
		}
		for _, ep := range distributors {
			resp, err := t.announce(ep, localEndpoint)
			if err != nil {
				log.Println("announce failed:", err)
				continue
			}
			t.Lock()
			t.distributors[resp.NodeID] = ep
			t.Unlock()
		}
	}
}

func (t *nodeServerLogic) announce(remote, localEndpoint tasks.Endpoint) (tasks.AnnounceResponse, error) {
	resp := tasks.AnnounceResponse{}
	err := tasks.MakeHTTPAPIRequest(t.c,
		remote,
		tasks.DistributorPostAnnounce,
		url.Values{"ip": []string{localEndpoint.IP.String()}, "nodeID": []string{string(t.id)}, "port": []string{fmt.Sprint(localEndpoint.Port)}},
		nil,
		&resp)
	if err != nil {
		return tasks.AnnounceResponse{}, errors.Wrap(err, "unable to announce to master node")
	}

	return resp, nil
}

func (t *nodeServerLogic) getOrCreateContainer(w tasks.WorkerID) *taskContainer {
	t.tasksToExecuteLocallyLock.RLock()
	if container, ok := t.tasksToExecuteLocally[w]; ok {
		t.tasksToExecuteLocallyLock.RUnlock()
		return container
	}
	t.tasksToExecuteLocallyLock.RUnlock()
	t.tasksToExecuteLocallyLock.Lock()
	if container, ok := t.tasksToExecuteLocally[w]; ok {
		t.tasksToExecuteLocallyLock.Unlock()
		return container
	}
	container := &taskContainer{
		tasks: make([]tasks.Task, 0),
	}
	t.tasksToExecuteLocally[w] = container
	t.tasksToExecuteLocallyLock.Unlock()
	return container
}

func (t *nodeServerLogic) GetTasks(w tasks.WorkerID, numWant int) ([]tasks.Task, error) {
	container := t.getOrCreateContainer(w)
	container.Lock()

	if len(container.tasks) > 0 {
		if numWant > len(container.tasks) {
			goto request
		}

		tasksToReturn := container.tasks[:numWant]
		if numWant == len(container.tasks) {
			container.tasks = make([]tasks.Task, 0)
		} else {
			container.tasks = container.tasks[numWant+1:]
		}
		container.timestamp = time.Now()
		container.Unlock()
		t.markRunning(tasksToReturn)
		return tasksToReturn, nil
	}
	container.Unlock()

request:
	select {
	case <-t.shutdown:
		return nil, errors.New("shutting down")
	default:
	}

	var endpoints []tasks.Endpoint
	t.RLock()
	for _, ep := range t.distributors {
		endpoints = append(endpoints, ep)
	}
	t.RUnlock()

	var requestedTasks []tasks.Task
	var err error
	requestNumWant := t.cfg.NumWantTasks
	if numWant > requestNumWant {
		requestNumWant = numWant
	}

	for _, ep := range endpoints {
		requestedTasks, err = t.getTasksFromEndpoint(ep, requestNumWant)
		if err != nil {
			log.Println(err)
		}
		if len(requestedTasks) > 0 {
			break
		}
	}

	if len(requestedTasks) == 0 {
		time.Sleep(time.Second)
		goto request
	}

	container.Lock()
	container.tasks = append(container.tasks, requestedTasks...)
	var tasksToReturn []tasks.Task
	if numWant >= len(container.tasks) {
		tasksToReturn = container.tasks
		container.tasks = make([]tasks.Task, 0)
	} else {
		tasksToReturn = container.tasks[:numWant]
		container.tasks = container.tasks[numWant+1:]
	}

	container.timestamp = time.Now()
	container.Unlock()

	t.markRunning(tasksToReturn)
	return tasksToReturn, nil
}

func (t *nodeServerLogic) getTasksFromEndpoint(endpoint tasks.Endpoint, numWant int) ([]tasks.Task, error) {
	var taskList []tasks.Task
	err := tasks.MakeHTTPAPIRequest(t.c,
		endpoint,
		tasks.DistributorGetTasks,
		url.Values{"timeout": []string{fmt.Sprint(int(t.cfg.GetTaskLongPollingTimeout.Seconds()))},
			"nodeID":  []string{string(t.id)},
			"numWant": []string{fmt.Sprint(numWant)}},
		nil,
		&taskList)
	if err != nil {
		return nil, errors.Wrap(err, "unable to request tasks from distributor")
	}

	return taskList, nil
}

func (t *nodeServerLogic) markRunning(tasks []tasks.Task) error {
	t.runningTasksLock.Lock()
	defer t.runningTasksLock.Unlock()

	for _, task := range tasks {
		if _, ok := t.runningTasks[task.ID]; ok {
			log.Printf("attempted to mark already running task %s as running", task.ID)
			return ErrAlreadyRunning
		}

		t.runningTasks[task.ID] = timestampedTask{Task: task, timestamp: time.Now()}
	}
	return nil
}

func (t *nodeServerLogic) HandIn(taskID tasks.TaskID, status tasks.ExecutionStatus) error {
	t.runningTasksLock.Lock()
	defer t.runningTasksLock.Unlock()

	if _, ok := t.runningTasks[taskID]; !ok {
		return ErrUnknownTask
	}

	delete(t.runningTasks, taskID)
	t.handInsToReport <- handInEntry{taskID: taskID, status: status}

	return nil
}

func (t *nodeServerLogic) handInLoop() {
	handinBuffer := make([]handInEntry, 0, t.cfg.HandInBufferSize)
	deadline := time.NewTimer(t.cfg.HandInFlushInterval)
	for {
		select {
		case <-t.shutdown:
			if len(handinBuffer) > 0 {
				err := t.flushHandIns(handinBuffer)
				if err != nil {
					log.Println("unable to report handins:", err.Error())
				}
				handinBuffer = handinBuffer[:0]
			}
			return
		case <-deadline.C:
			if len(handinBuffer) > 0 {
				err := t.flushHandIns(handinBuffer)
				if err != nil {
					log.Println("unable to report handins:", err.Error())
				}
				handinBuffer = handinBuffer[:0]
			}

			// reset timer
			deadline.Reset(t.cfg.HandInFlushInterval)
		case handin := <-t.handInsToReport:
			handinBuffer = append(handinBuffer, handin)
			if len(handinBuffer) == t.cfg.HandInBufferSize {
				err := t.flushHandIns(handinBuffer)
				if err != nil {
					log.Println("unable to report handins:", err.Error())
				}
				handinBuffer = handinBuffer[:0]

				// reset timer
				if !deadline.Stop() {
					<-deadline.C
				}
				deadline.Reset(t.cfg.HandInFlushInterval)
			}
		}
	}
}

func (t *nodeServerLogic) flushHandIns(handins []handInEntry) error {
	sortedHandins := make(map[pyme.NodeID][]handInEntry)
	for _, entry := range handins {
		node := entry.taskID.NodeID()
		if _, ok := sortedHandins[node]; !ok {
			sortedHandins[node] = make([]handInEntry, 0, 1)
		}
		sortedHandins[node] = append(sortedHandins[node], entry)
	}

	var pErr error
	for distributor, entries := range sortedHandins {
		err := t.flushHandinsByDistributor(entries, distributor)
		if err != nil {
			if pErr != nil {
				log.Println(pErr)
			}
			pErr = err
		}
	}

	return pErr
}

func (t *nodeServerLogic) flushHandinsByDistributor(handins []handInEntry, distributor pyme.NodeID) error {
	t.RLock()
	ep, ok := t.distributors[distributor]
	t.RUnlock()
	if !ok {
		return fmt.Errorf("unknown distributor: %s", distributor)
	}

	serializableHandins := make([]tasks.SerializableHandInEntry, 0, len(handins))
	for _, h := range handins {
		serializableHandins = append(serializableHandins, tasks.SerializableHandInEntry{TaskID: h.taskID, ExecutionStatus: string(h.status)})
	}

	return tasks.MakeHTTPAPIRequest(t.c,
		ep,
		tasks.DistributorPostHandIn,
		url.Values{"nodeID": []string{string(t.id)}},
		serializableHandins,
		nil)
}
