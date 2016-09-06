package distributor

import (
	"context"
	"log"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/mrd0ll4r/pyme"
	"github.com/mrd0ll4r/pyme/tasks"
)

type ratingRequest struct {
	task tasks.Task
	c    chan float64
}

type node struct {
	id             pyme.NodeID
	ip             net.IP
	port           uint16

	taskQueue      []tasks.Task
	givenOut       map[tasks.TaskID]tasks.Task
	stats          tasks.NodeStatistics
	ratingRequests chan ratingRequest
	c              *http.Client

	// announce is to be fed with announces
	announce       chan time.Time

	// timeout will be closed if the node times out
	timeoutChan    chan struct{}

	// isInactive is closed while the node is inactive
	isInactive     chan struct{}
	activityChange sync.RWMutex

	cfg            Config

	sync.RWMutex
}

func newNode(id pyme.NodeID, ip net.IP, port uint16, cfg Config) *node {
	toReturn := &node{
		id:             id,
		ip:             ip,
		port:           port,
		taskQueue:      make([]tasks.Task, 0, 1000),
		givenOut:       make(map[tasks.TaskID]tasks.Task),
		ratingRequests: make(chan ratingRequest, 1000),
		c: &http.Client{
			Timeout: cfg.HTTPClientTimeout,
		},
		announce:    make(chan time.Time),
		isInactive:  make(chan struct{}),
		timeoutChan: make(chan struct{}),
		cfg:         cfg,
	}
	toReturn.activate()

	go toReturn.rateLoop()

	return toReturn
}

func (n *node) updateEndpoint(ip net.IP, port uint16) {
	n.Lock()
	defer n.Unlock()
	n.ip = ip
	n.port = port
}

func (n *node) deactivate() []tasks.Task {
	n.activityChange.Lock()
	defer n.activityChange.Unlock()
	close(n.isInactive)
	close(n.announce)
	return n.withdrawAll()
}

func (n *node) activate() {
	n.announce = make(chan time.Time)
	n.timeoutChan = make(chan struct{})
	n.isInactive = make(chan struct{})
	go n.announceLoop()
}

func (n *node) timeout() <-chan struct{} {
	return n.timeoutChan
}

func (n *node) withdrawAll() []tasks.Task {
	toReturn := make([]tasks.Task, 0, 1000)

	n.Lock()
	defer n.Unlock()
	toReturn = append(toReturn, n.taskQueue...)
	n.taskQueue = make([]tasks.Task, 0, 1000)
	// this leaves the underlying array allocated, which makes enqueueing
	// new tasks faster, but doesn't release the memory.
	//n.taskQueue = n.taskQueue[:0]

	// collect already given out
	for id, task := range n.givenOut {
		toReturn = append(toReturn, task)
		delete(n.givenOut, id)
		n.stats.TasksNotExecuted++
	}

	return toReturn
}

func (n *node) enqueue(task tasks.Task) error {
	n.activityChange.RLock()
	defer n.activityChange.RUnlock()

	select {
	case <-n.isInactive:
		return errors.New("node inactive")
	default:
	}

	n.Lock()
	k := sort.Search(len(n.taskQueue), func(i int) bool {
		return strings.Compare(string(task.ID), string(n.taskQueue[i].ID)) >= 0
	})
	if k < len(n.taskQueue) && n.taskQueue[k].ID == task.ID {
		n.Unlock()
		return errors.New("duplicate task")
	}

	// insert at correct position to keep ordering
	n.taskQueue = append(n.taskQueue, tasks.Task{})
	copy(n.taskQueue[k+1:], n.taskQueue[k:])
	n.taskQueue[k] = task

	n.Unlock()
	return nil
}

func (n *node) markGivenOut(tasks []tasks.Task) {
	n.Lock()
	defer n.Unlock()

	for _, task := range tasks {
		n.givenOut[task.ID] = task
	}

	n.stats.TasksAccepted += uint64(len(tasks))
}

func (n *node) getTasks(numWant int, timeout time.Duration) ([]tasks.Task, error) {
	n.activityChange.RLock()
	defer n.activityChange.RUnlock()

	select {
	case <-n.isInactive:
		// node is inactive, do nothing
		return []tasks.Task{}, errors.New("inactive node")
	default:
	}

	deadline := time.After(timeout)
	if numWant <= 0 {
		numWant = n.cfg.DefaultNumWant
	}

	tasks := make([]tasks.Task, 0, numWant)

	for {
		select {
		case <-deadline:
			n.markGivenOut(tasks)
			return tasks, nil
		default:
		}
		// no timeout, let's look at the buffer

		n.Lock()
		if len(n.taskQueue) > 0 {
			take := numWant - len(tasks)
			if take > len(n.taskQueue) {
				take = len(n.taskQueue)
			}

			for i := 0; i < take; i++ {
				tasks = append(tasks, n.taskQueue[i])
			}

			n.taskQueue = n.taskQueue[take:]

			n.Unlock()
		} else {
			n.Unlock()
			time.Sleep(100 * time.Millisecond)
			runtime.Gosched()
			continue
		}

		if len(tasks) == numWant {
			n.markGivenOut(tasks)
			return tasks, nil
		}
	}
}

var errUntrackedTask = errors.New("untracked task")

func (n *node) handIn(taskID tasks.TaskID, status tasks.ExecutionStatus) (tasks.Task, error) {
	n.Lock()
	defer n.Unlock()

	task, ok := n.givenOut[taskID]
	if !ok {
		return tasks.Task{}, errUntrackedTask
	}
	delete(n.givenOut, taskID)

	switch status {
	case tasks.Success:
		n.stats.TasksCompleted++
	case tasks.Failure:
		n.stats.TasksFailed++
	case tasks.NotExecuted:
		n.stats.TasksNotExecuted++
	}

	return task, nil
}

func (n *node) announceLoop() {
	for {
		select {
		case <-time.After(n.cfg.NodeTimeout):
			// node timed out
			close(n.timeoutChan)
			return
		case <-n.announce:
			// got announce, do nothing, reset timer
		}
	}
}

func (n *node) rate(ctx context.Context, task tasks.Task) <-chan float64 {
	c := make(chan float64)
	req := ratingRequest{
		task: task,
		c:    c,
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case n.ratingRequests <- req:
		}

		return
	}()
	return c
}

func (n *node) rateLoop() {
	toRateBuffer := make([]ratingRequest, 0, n.cfg.RatingBufferSize)
	deadline := time.NewTimer(n.cfg.RatingBufferFlushInterval)
	for {
		select {
		case <-deadline.C:
			if len(toRateBuffer) > 0 {
				bufferCopy := make([]ratingRequest, len(toRateBuffer))
				copy(bufferCopy, toRateBuffer)
				go func() {
					err := n.flushRatingRequests(bufferCopy)
					if err != nil {
						log.Println("unable to request ratings:", err.Error())
					}
				}()
				toRateBuffer = toRateBuffer[:0]
			}

			// reset timer
			deadline.Reset(n.cfg.RatingBufferFlushInterval)
		case rateRequest := <-n.ratingRequests:
			toRateBuffer = append(toRateBuffer, rateRequest)
			if len(toRateBuffer) == n.cfg.RatingBufferSize {
				bufferCopy := make([]ratingRequest, len(toRateBuffer))
				copy(bufferCopy, toRateBuffer)
				go func() {
					err := n.flushRatingRequests(bufferCopy)
					if err != nil {
						log.Println("unable to request ratings:", err.Error())
					}
				}()
				toRateBuffer = toRateBuffer[:0]

				// reset timer
				if !deadline.Stop() {
					<-deadline.C
				}
				deadline.Reset(n.cfg.RatingBufferFlushInterval)
			}
		}
	}
}

func (n *node) flushRatingRequests(requests []ratingRequest) error {
	tasks := make([]tasks.Task, 0, len(requests))
	for _, req := range requests {
		tasks = append(tasks, req.task)
	}

	ratings := make([]tasks.TaskRating, 0, len(requests))

	n.RLock()
	err := tasks.MakeHTTPAPIRequest(n.c, tasks.Endpoint{IP: n.ip, Port: n.port}, tasks.NodeServerPostRate, nil, tasks, &ratings)
	n.RUnlock()
	if err != nil {
		return errors.Wrap(err, "unable to perform rating request")
	}

	go func() {
		for _, rating := range ratings {
			i := 0
			for _, req := range requests {
				if req.task.ID == rating.ID {
					i++
				}
			}

			if i != 1 {
				log.Printf("rating: task %s was rated %d times (expected 1), node %s, received %d ratings in this batch, requested %d", rating.ID, i, n.id, len(ratings), len(requests))
				continue
			}

			found := false
			for _, req := range requests {
				if req.task.ID == rating.ID {
					req.c <- rating.Cost
					close(req.c)
					found = true
					break
				}
			}
			if !found {
				log.Printf("unmatched rating request for task %s - skipping", rating.ID)
			}
		}
	}()

	return nil
}
