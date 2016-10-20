package tasks

import (
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/mrd0ll4r/pyme"
)

type errUnknownExecutionStatus string

func (e errUnknownExecutionStatus) Error() string {
	return fmt.Sprintf("unknown execution status: %s", string(e))
}

type errUnknownTaskType string

func (e errUnknownTaskType) Error() string {
	return fmt.Sprintf("unknown task type: %s", string(e))
}

// WorkerID is a node-unique identifier for a worker.
type WorkerID string

// TaskID is a unique identifier for a task.
// It consists of the NodeID of the distributor node followed by a dash, the
// name of the queue this task belongs to, followed by a dash and a unique
// identifier within the queue.
//
// An example TaskID could look like this:
// hU5k4sAeOnAhf93nD8f5-series5025-loc25409
type TaskID string

// NodeID extracts the distributor node's ID from the TaskID.
func (t TaskID) NodeID() pyme.NodeID {
	return pyme.NodeID(strings.Split(string(t), "-")[0])
}

// Endpoint describes a TCP endpoint on a single machine.
type Endpoint struct {
	// IP is the IP of the machine.
	IP net.IP
	// Port is the TCP port of the endpoint.
	Port uint16
}

// Queue extracts the queue name from the TaskID.
// If the TaskID is malformed or the queue name can not be extracted the whole
// TaskID is returned.
func (t TaskID) Queue() string {
	split := strings.Split(string(t), "-")
	if len(split) > 1 {
		return split[1]
	}
	return string(t)
}

// TaskIdentifier extracts the queue-local task identifier from the TaskID.
// If the TaskID is malformed or the task identifier can not be extracted the
// whole TaskID is returned.
func (t TaskID) TaskIdentifier() string {
	split := strings.Split(string(t), "-")
	if len(split) > 2 {
		return split[2]
	}
	return string(t)
}

// ExecutionStatus is the status of a task that has been distributed.
type ExecutionStatus string

// ExecutionStatus constants.
const (
	// Success implies that the task has been executed and all results have
	// been reported.
	Success ExecutionStatus = "success"

	// Failure indicates that the execution of the task has failed.
	Failure = "failure"

	// NotExecuted indicates that the task has not been executed.
	NotExecuted = "notExecuted"

	// UnknownExecutionStatus indicates an invalid value.
	UnknownExecutionStatus = "unknown"
)

// ExecutionStatusFromString tries to determine the execution status indicated
// by s and returns an error if the execution status is unknown.
func ExecutionStatusFromString(s string) (ExecutionStatus, error) {
	switch {
	case compareIgnoreCase(s, string(Success)):
		return Success, nil
	case compareIgnoreCase(s, string(Failure)):
		return Failure, nil
	case compareIgnoreCase(s, string(NotExecuted)):
		return NotExecuted, nil
	}

	return UnknownExecutionStatus, errUnknownExecutionStatus(s)
}

func compareIgnoreCase(a, b string) bool {
	return strings.ToLower(a) == strings.ToLower(b)
}

// SerializableHandInEntry describes a change in the execution status of a task.
// The ExecutionStatus must not be assumed to be a valid ExecutionStatus and
// must be parsed using ExecutionStatusFromString before use.
type SerializableHandInEntry struct {
	// TaskID is the TaskID of the referenced Task.
	TaskID TaskID `json:"taskID"`

	// ExecutionStatus
	ExecutionStatus string `json:"status"`
}

// TaskType is the type of a task.
type TaskType string

// TaskType constants.
const (
	// Localization tasks localize events in a frame.
	Localization TaskType = "localization"

	// Reconstruction tasks reconstruction an image from localization data.
	Reconstruction = "reconstruction"

	// Recipe tasks are generic image-processing recipes.
	Recipe = "recipe"

	// UnknownTaskType indicates an invalid TaskType.
	UnknownTaskType = "unknown"
)

// TaskTypeFromString tries to determine the task type from s and returns an
// error if the task type is unknown.
func TaskTypeFromString(s string) (TaskType, error) {
	switch {
	case compareIgnoreCase(s, string(Localization)):
		return Localization, nil
	case compareIgnoreCase(s, string(Reconstruction)):
		return Reconstruction, nil
	case compareIgnoreCase(s, string(Recipe)):
		return Recipe, nil
	}

	return UnknownTaskType, errUnknownTaskType(s)
}

// Task represents a single task to be executed.
type Task struct {
	// ID identifies the task uniquely.
	ID TaskID `json:"id"`

	// Type determines the type of the task.
	Type TaskType `json:"type"`

	// Taskdef is the definition of a task.
	// It must not be present if TaskdefRef is not empty.
	Taskdef map[string]string `json:"taskdef,omitempty"`

	// TaskdefRef references the task definition through a resource URI.
	// It must not be present if Taskdef is not nil.
	TaskdefRef string `json:"taskdefRef,omitempty"`

	// Inputs describes how to obtain input data.
	Inputs map[string]string `json:"inputs"`

	// Outputs describes how to store results.
	Outputs map[string]string `json:"outputs"`
}

// QueueStatistics represent statistics about a queue.
type QueueStatistics struct {
	// TasksPosted is the number of tasks posted to the queue.
	TasksPosted uint64 `json:"tasksPosted"`

	// TasksRunning is the number of tasks that have been rated and
	// assigned to a node.
	TasksRunning uint64 `json:"tasksRunning"`

	// TasksCompleted is the number of tasks that have completed
	// successfully.
	TasksCompleted uint64 `json:"tasksCompleted"`

	// TasksFailed is the number of tasks that have failed.
	TasksFailed uint64 `json:"tasksFailed"`

	// AverageExecutionCost is the average execution cost of a task in this
	// queue, determined by the execution cost known at the time of
	// assigning the task to a node.
	AverageExecutionCost float64 `json:"averageExecutionCost"`
}

// NodeStatistics represent statistics about a node.
type NodeStatistics struct {
	// TasksAccepted is the number of tasks the node has received as a
	// result of calling GetTasks.
	TasksAccepted uint64 `json:"tasksAccepted"`

	// TasksCompleted is the number of tasks the node has reported as
	// executed successfully.
	TasksCompleted uint64 `json:"tasksCompleted"`

	// TasksCompleted is the number of tasks the node has reported as
	// failed.
	TasksFailed uint64 `json:"tasksFailed"`

	// TasksCompleted is the sum of the number of tasks the node has
	// reported as not executed and tasks that were withdrawn from a node
	// as a result of the node timing out.
	TasksNotExecuted uint64 `json:"tasksNotExecuted"`
}

// TaskRating represents the rating of a task for a single node.
type TaskRating struct {
	// ID is the TaskID of the referenced task.
	ID TaskID `json:"id"`

	// Cost is the estimated execution cost for the referenced task.
	Cost float64 `json:"cost"`
}

// Calculator is the interface for a calculator.
// A calculator calculates the execution costs for a certain type of tasks by
// constructing URIs for all required resources and calling a Rater for each
// of them.
type Calculator interface {
	// Calculate estimates the execution cost of a task.
	Calculate(Task) (float64, error)

	// TaskType returns the TaskType this calculator is responsible for.
	TaskType() TaskType
}

// Rater is the interface for a rater.
// A rater estimates the cost of fetching or storing a resource identified by
// a URI.
// A rater is responsible for one URI scheme.
type Rater interface {
	// Rate estimates the cost of fetching or storing the resource identified
	// by URI.
	Rate(URI *url.URL) (float64, error)

	// Scheme returns the URI scheme this rater is responsible for.
	Scheme() string
}

// AnnounceResponse represents the response to an Announce.
type AnnounceResponse struct {
	// NodeID is the NodeID of the distributor.
	NodeID pyme.NodeID `json:"nodeID"`
}

// NodeServer describes the node-local server for task distribution.
type NodeServer interface {
	// GetTasks gets up to numWant task to be executed by a worker.
	// numWant <= 0 defaults to one task.
	GetTasks(id WorkerID, numWant int) ([]Task, error)

	// HandIn notifies the task server that the execution of the task with
	// the given TaskID is finished.
	// If the ExecutionStatus is Success, the task has been completed and
	// all results have been submitted.
	// If the ExecutionStatus is Failure, the task server can decide whether
	// to report the failure to the node that assigned the task or whether
	// to try executing it again on a local worker.
	HandIn(TaskID, ExecutionStatus) error

	// RateTask rates the given task for this node.
	// Rating must assign a value in [0,1] for each resource necessary
	// to complete the task. The rating for the complete task must be in
	// [0,n] for n resources required to execute the task or -1.
	// A rating of -1 indicates that this node does not rate tasks at the
	// moment and rating should be done on the distributor instead.
	RateTask(Task) (TaskRating, error)

	// Stop stops the NodeServer.
	// This will stop any open requests for GetTask and will stop announcing
	// regularly to its Distributors. They will register the node as being
	// inactive and redistribute tasks given to it.
	Stop()
}

// TaskDistributor describes the central instance for task distribution.
type TaskDistributor interface {
	// PostTask adds the given task to the set of tasks to be processed.
	// The task will be assigned to a node based on the location of the data
	// needed to process the task.
	// The ID of the given task needs to consist only of the unique
	// identifier within the queue. The queue name and distributor node's ID
	// will be prepended by the distributor.
	// This is typically called from the analysis software.
	PostTask(queue string, task Task) error

	// GetTasks gets up to numWant tasks to be executed on the node identified
	// by nodeID.
	// The call blocks until tasks are available or timeout has elapsed.
	// It is not guaranteed that numWant tasks will be returned. The task
	// server may (and should!) hold tasks for a node in a queue and can
	// return multiple tasks at once.
	// A task server is allowed to request more tasks than it immediately
	// needs, i.e. more tasks than it has workers to give them to. If the
	// task server shuts down or otherwise stops executing tasks, it must
	// report tasks not executed to the node that reported them with an
	// ExecutionStatus of NotExecuted.
	GetTasks(nodeID pyme.NodeID, numWant int, timeout time.Duration) ([]Task, error)

	// HandIn notifies the task server of the ExecutionStatus of the task
	// identified by the given TaskID.
	// If the ExecutionStatus is Success, the task has been completed and
	// all results have been submitted.
	// If the ExecutionStatus is NotExecuted, the task has not been
	// attempted to execute and must be redistributed to the cluster.
	// Including the NodeID prevents nodes from wrongfully handing in tasks
	// for another node.
	HandIn(pyme.NodeID, TaskID, ExecutionStatus) error

	// QueueStatistics returns statistics about queues running on the
	// Distributor.
	QueueStatistics() map[string]QueueStatistics

	// Announce announces the endpoint of a local node server to a
	// distributor.
	// A node has to reannounce at a certain interval - if a distributor
	// does not receive an announce in a set amount of time it presumes
	// the node defunct.
	// It will be taken out of the set of active nodes and all tasks
	// enqueued for this node will be redistributed.
	Announce(nodeID pyme.NodeID, ip net.IP, port uint16) (AnnounceResponse, error)
}
