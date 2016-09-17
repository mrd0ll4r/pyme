package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"

	"github.com/mrd0ll4r/pyme"
)

type returnFormat int

// format constants.
const (
	JSON returnFormat = iota
	BSON
)

var (
	emptyIPv4 = net.IP([]byte{0, 0, 0, 0})
	emptyIPv6 = net.IP([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
)

// responseFunc is a function that handles an API request and returns an HTTP
// status code, an optional result to be embedded and an error.
type responseFunc func(*http.Request, httprouter.Params) (status int, result interface{}, err error)

// noResultResponseFunc is a function that handles an API request and returns an
// HTTP status code and an error.
type noResultResponseFunc func(*http.Request, httprouter.Params) (status int, err error)

// ErrInternalServerError is the error used for recovered API calls.
var ErrInternalServerError = errors.New("internal server error")

// ErrNoNodeID is returned if no node ID was specified.
var ErrNoNodeID = errors.New("no node ID")

// ErrNoIP is returned if no IP was specified.
var ErrNoIP = errors.New("no IP")

// ErrInvalidIP is returned if an invalid IP was specified.
var ErrInvalidIP = errors.New("invalid IP")

// ErrNoPort is returned if no port was specified.
var ErrNoPort = errors.New("no port")

// ErrInvalidPort is returned if an invalid port was specified.
var ErrInvalidPort = errors.New("invalid port")

// ErrNoNumWant is returned if no numWant was specified.
var ErrNoNumWant = errors.New("no numWant")

// ErrInvalidNumWant is returned if an invalid numWant was specified.
var ErrInvalidNumWant = errors.New("invalid numWant")

// ErrNoTimeout is returned if no timeout was specified.
var ErrNoTimeout = errors.New("no timeout")

// ErrInvalidTimeout is returned if an invalid timeout was specified.
var ErrInvalidTimeout = errors.New("invalid timeout")

// ErrNoTaskID is returned if no task ID was specified.
var ErrNoTaskID = errors.New("no task ID")

// ErrNoWorkerID is returned if no worker ID was specified.
var ErrNoWorkerID = errors.New("no worker ID")

// ErrNoQueue is returned if no queue was specified.
var ErrNoQueue = errors.New("no queue")

// ErrNoExecutionStatus is returned if no execution status was specified.
var ErrNoExecutionStatus = errors.New("no execution status")

// response is the response type for all API queries.
type response struct {
	Ok     bool        `json:"ok"`
	Error  string      `json:"error,omitempty"`
	Result interface{} `json:"result,omitempty"`
}

// makeHandler wraps a responseFunc to make a httprouter.Handle.
func makeHandler(inner responseFunc, apiKey string, debug bool) httprouter.Handle {
	if apiKey != "" {
		inner = authorizationHandler(inner, apiKey)
	}
	handler := logHandler(recoverHandler(inner), debug)

	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		resp := response{}

		status, result, err := handler(r, p)
		if err != nil {
			resp.Error = err.Error()
		} else {
			resp.Ok = true
		}
		if result != nil {
			resp.Result = result
		}

		err = encodeResponse(w, r, p, resp, status)
		if err != nil {
			log.Println("API: unable to send response:", err)
		}
	}
}

func encodeResponse(w http.ResponseWriter, r *http.Request, p httprouter.Params, resp response, status int) (err error) {
	switch determineReturnFormat(r) {
	case JSON:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		w.WriteHeader(status)

		err = json.NewEncoder(w).Encode(resp)
	case BSON:
		err = errors.New("BSON not implemented yet")
	default:
		err = errors.New("unknown format")
	}

	return
}

func determineReturnFormat(r *http.Request) returnFormat {
	f := r.URL.Query().Get("format")
	if f == "" {
		return JSON
	}

	switch strings.ToLower(f) {
	case "json":
		return JSON
	case "bson":
		return BSON
	default:
		return JSON
	}
}

// authorizationHandler wraps the responseFunc in an authorization handler
// that expects an API key and terminates the request if no key or an invalid
// key was specified.
func authorizationHandler(inner responseFunc, apiKey string) responseFunc {
	return func(r *http.Request, p httprouter.Params) (int, interface{}, error) {
		token := getAPIKey(r)
		if token != apiKey {
			return http.StatusForbidden, nil, errors.New("invalid API key")
		}

		return inner(r, p)
	}
}

func getAPIKey(r *http.Request) string {
	token := r.Header.Get("X-API-Key")

	if token == "" {
		token = r.URL.Query().Get("apikey")
	}

	return token
}

// logHandler wraps the responseFunc in a logging handler.
// If debug is true all requests will be logged.
// Erroneous requests are always logged.
func logHandler(inner responseFunc, debug bool) responseFunc {
	return func(r *http.Request, p httprouter.Params) (int, interface{}, error) {
		before := time.Now()

		status, result, err := inner(r, p)
		delta := time.Since(before)

		if debug && err == nil {
			log.Printf("%d %s %s %s %s %s", status, delta.String(), r.RemoteAddr, r.Method, r.URL.EscapedPath(), r.URL.Query().Encode())
		}

		if err != nil {
			log.Printf("%d %s %s %s %s (%s)", status, delta.String(), r.RemoteAddr, r.Method, r.URL.EscapedPath(), err.Error())
		}

		return status, result, err
	}
}

// recoverHandler wraps the responseFunc in a recovery handler.
// The handler will recover any panic that occurred in the inner responseFunc
// and return a sanitized status code, no result and ErrInternalServerError.
func recoverHandler(inner responseFunc) responseFunc {
	return func(r *http.Request, p httprouter.Params) (status int, result interface{}, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Println("API: recovered:", rec)
				status = http.StatusInternalServerError
				result = nil
				err = ErrInternalServerError
			}
		}()

		status, result, err = inner(r, p)
		return
	}
}

// noResultHandler wraps a noResultResponseFunc to be a responseFunc, returning
// nil for the result.
func noResultHandler(inner noResultResponseFunc) responseFunc {
	return func(r *http.Request, p httprouter.Params) (int, interface{}, error) {
		status, err := inner(r, p)

		return status, nil, err
	}
}

func (s *HTTPDistributorServer) handleGetTasks(r *http.Request, p httprouter.Params) (int, interface{}, error) {
	nodeID, err := getNodeID(r)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to determine node ID")
	}

	timeout, err := getTimeout(r)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to determine timeout")
	}

	numWant, err := getNumWant(r)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to determine numWant")
	}

	tasks, err := s.s.GetTasks(nodeID, numWant, timeout)
	if err != nil {
		return 400, nil, err
	}

	return 200, tasks, nil
}

func (s *HTTPDistributorServer) handleGetQueues(r *http.Request, p httprouter.Params) (int, interface{}, error) {
	stats := s.s.QueueStatistics()

	return 200, stats, nil
}

func (s *HTTPDistributorServer) handlePostAnnounce(r *http.Request, p httprouter.Params) (int, interface{}, error) {
	nodeID, err := getNodeID(r)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to determine node ID")
	}

	ip, err := getIP(r)
	if err != nil || ip.Equal(emptyIPv4) || ip.Equal(emptyIPv6) {
		// no IP, invalid IP or empty IP
		ipString, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return 400, nil, errors.Wrap(err, "unable to determine node IP")
		}
		ip = net.ParseIP(ipString)
		if len(ip) == 0 {
			panic(fmt.Sprintf("unable to parse HTTP RemoteAddr %q", r.RemoteAddr))
		}
	}

	port, err := getPort(r)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to determine node port")
	}

	resp, err := s.s.Announce(nodeID, ip, port)
	if err != nil {
		return 400, nil, err
	}

	return 200, resp, nil
}

func (s *HTTPDistributorServer) handlePostHandIn(r *http.Request, p httprouter.Params) (int, error) {
	nodeID, err := getNodeID(r)
	if err != nil {
		return 400, errors.Wrap(err, "unable to determine node ID")
	}

	var entries []SerializableHandInEntry
	d := json.NewDecoder(r.Body)
	err = d.Decode(&entries)
	if err != nil {
		return 400, errors.Wrap(err, "unable to parse handins")
	}

	for _, h := range entries {
		if len(h.TaskID) == 0 {
			return 400, errors.New("missing task ID")
		}

		status, err := ExecutionStatusFromString(h.ExecutionStatus)
		if err != nil {
			return 400, errors.Wrap(err, "unable to parse handins")
		}

		err = s.s.HandIn(nodeID, h.TaskID, status)
		if err != nil {
			return 400, err
		}
	}

	return 200, nil
}

func (s *HTTPDistributorServer) handlePostTasks(r *http.Request, p httprouter.Params) (int, error) {
	queue, err := getQueue(r)
	if err != nil {
		return 400, errors.Wrap(err, "unable to determine queue")
	}

	var tasks []Task
	d := json.NewDecoder(r.Body)
	err = d.Decode(&tasks)
	if err != nil {
		return 400, errors.Wrap(err, "unable to parse tasks")
	}

	for _, t := range tasks {
		err = s.s.PostTask(queue, t)
		if err != nil {
			return 400, errors.Wrap(err, "unable to post task")
		}
	}

	return 200, nil
}

func (s *HTTPNodeServer) handleGetTasks(r *http.Request, p httprouter.Params) (int, interface{}, error) {
	workerID, err := getWorkerID(r)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to determine worker ID")
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	task, err := s.s.GetTask(ctx, workerID)
	if err != nil {
		return 400, nil, err
	}

	return 200, task, nil
}

func (s *HTTPNodeServer) handlePostHandIn(r *http.Request, p httprouter.Params) (int, error) {
	taskID, err := getTaskID(r)
	if err != nil {
		return 400, errors.Wrap(err, "unable to determine task ID")
	}

	execStatus, err := getExecutionStatus(r)
	if err != nil {
		return 400, errors.Wrap(err, "unable to determine execution status")
	}

	err = s.s.HandIn(taskID, execStatus)
	if err != nil {
		return 400, err
	}

	return 200, nil
}

func (s *HTTPNodeServer) handlePostRate(r *http.Request, p httprouter.Params) (int, interface{}, error) {
	var tasks []Task
	d := json.NewDecoder(r.Body)
	err := d.Decode(&tasks)
	if err != nil {
		return 400, nil, errors.Wrap(err, "unable to parse tasks")
	}

	ratings := make([]TaskRating, 0, len(tasks))

	for _, t := range tasks {
		rating, err := s.s.RateTask(t)
		if err != nil {
			return 400, nil, errors.Wrap(err, "unable to rate task")
		}
		ratings = append(ratings, rating)
	}

	return 200, ratings, nil
}

func getExecutionStatus(r *http.Request) (ExecutionStatus, error) {
	s := r.URL.Query().Get("status")
	if s == "" {
		return ExecutionStatus(""), ErrNoExecutionStatus
	}

	return ExecutionStatusFromString(s)
}

func getTaskID(r *http.Request) (TaskID, error) {
	n := r.URL.Query().Get("taskID")
	if n == "" {
		return TaskID(""), ErrNoTaskID
	}

	return TaskID(n), nil
}

func getWorkerID(r *http.Request) (WorkerID, error) {
	n := r.URL.Query().Get("workerID")
	if n == "" {
		return WorkerID(""), ErrNoWorkerID
	}

	return WorkerID(n), nil
}

func getQueue(r *http.Request) (string, error) {
	n := r.URL.Query().Get("queue")
	if n == "" {
		return "", ErrNoQueue
	}

	return n, nil
}

func getNodeID(r *http.Request) (pyme.NodeID, error) {
	n := r.URL.Query().Get("nodeID")
	if n == "" {
		return pyme.NodeID(""), ErrNoNodeID
	}

	return pyme.NodeID(n), nil
}

func getIP(r *http.Request) (net.IP, error) {
	n := r.URL.Query().Get("ip")
	if n == "" {
		return net.IP(nil), ErrNoIP
	}

	ip := net.ParseIP(n)
	if len(ip) == 0 {
		return nil, ErrInvalidIP
	}

	return ip, nil
}

func getPort(r *http.Request) (uint16, error) {
	n := r.URL.Query().Get("port")
	if n == "" {
		return 0, ErrNoPort
	}

	p, err := strconv.Atoi(n)
	if err != nil {
		return 0, ErrInvalidPort
	}

	if p > 65535 || p < 1 {
		return 0, ErrInvalidPort
	}

	return uint16(p), nil
}

func getNumWant(r *http.Request) (int, error) {
	n := r.URL.Query().Get("numWant")
	if n == "" {
		return 0, ErrNoNumWant
	}

	p, err := strconv.Atoi(n)
	if err != nil {
		return 0, ErrInvalidNumWant
	}

	return p, nil
}

func getTimeout(r *http.Request) (time.Duration, error) {
	t := r.URL.Query().Get("timeout")
	if t == "" {
		return time.Duration(0), ErrNoTimeout
	}

	tInt, err := strconv.Atoi(t)
	if err != nil {
		return time.Duration(0), errors.Wrap(err, "unable to parse timeout")
	}

	if tInt < 0 {
		return time.Duration(0), ErrInvalidTimeout
	}

	return time.Second * time.Duration(tInt), nil
}
