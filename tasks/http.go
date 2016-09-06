package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

// ErrUnknownMethod is returned for unknown API methods.
var ErrUnknownMethod = errors.New("unknown method")

// ErrContentWithGet is returned if it is attempted to post content with a
// method that uses HTTP GET.
var ErrContentWithGet = errors.New("content with GET")

const nodePrefix = "/node"
const distributorPrefix = "/distributor"

// Method describes an HTTP API method.
type Method struct {
	// HTTPMethod is the HTTP method (verb) for the method.
	HTTPMethod string
	// Path is the HTTP path for the method.
	Path string
}

// API Methods
var (
	NodeServerGetTasks      = Method{http.MethodGet, nodePrefix + "/tasks"}
	NodeServerPostHandIn    = Method{http.MethodPost, nodePrefix + "/handin"}
	NodeServerPostRate      = Method{http.MethodPost, nodePrefix + "/rate"}
	DistributorGetTasks     = Method{http.MethodGet, distributorPrefix + "/tasks"}
	DistributorPostTasks    = Method{http.MethodPost, distributorPrefix + "/tasks"}
	DistributorPostHandIn   = Method{http.MethodPost, distributorPrefix + "/handin"}
	DistributorPostAnnounce = Method{http.MethodPost, distributorPrefix + "/announce"}
	DistributorGetQueues    = Method{http.MethodGet, distributorPrefix + "/queues"}
)

func isKnownMethod(m Method) bool {
	switch m {
	case NodeServerGetTasks:
	case NodeServerPostHandIn:
	case NodeServerPostRate:
	case DistributorGetTasks:
	case DistributorPostTasks:
	case DistributorPostHandIn:
	case DistributorPostAnnounce:
	case DistributorGetQueues:
	default:
		return false
	}
	return true
}

// HTTPNodeServer is the HTTP wrapper for a NodeServer.
type HTTPNodeServer struct {
	s      NodeServer
	server *http.Server
}

// NewHTTPNodeServer creates a new HTTPNodeServer.
func NewHTTPNodeServer(s NodeServer, apiKey string, endpoint string, debug bool) *HTTPNodeServer {
	toReturn := &HTTPNodeServer{
		s: s,
		server: &http.Server{
			Addr:         endpoint,
			ReadTimeout:  3600 * time.Second,
			WriteTimeout: 3600 * time.Second,
		},
	}

	toReturn.server.Handler = toReturn.routes(apiKey, debug)

	return toReturn
}

// ListenAndServe starts serving requests on the specified endpoint.
func (s *HTTPNodeServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

// Stop stops the server.
func (s *HTTPNodeServer) Stop() {
	// TODO factor out the listener and close it here.
}

func (s *HTTPNodeServer) routes(apiKey string, debug bool) http.Handler {
	r := httprouter.New()

	r.Handle(NodeServerGetTasks.HTTPMethod, NodeServerGetTasks.Path, makeHandler(s.handleGetTasks, apiKey, debug))

	r.Handle(NodeServerPostHandIn.HTTPMethod, NodeServerPostHandIn.Path, makeHandler(noResultHandler(s.handlePostHandIn), apiKey, debug))

	r.Handle(NodeServerPostRate.HTTPMethod, NodeServerPostRate.Path, makeHandler(s.handlePostRate, apiKey, debug))

	return r
}

// HTTPDistributorServer is the HTTP wrapper for a Distributor.
type HTTPDistributorServer struct {
	s      TaskDistributor
	server *http.Server
}

// NewHTTPDistributorServer creates a new HTTPDistributorServer.
func NewHTTPDistributorServer(s TaskDistributor, apiKey string, endpoint string, debug bool) *HTTPDistributorServer {
	toReturn := &HTTPDistributorServer{
		s: s,
		server: &http.Server{
			Addr:         endpoint,
			ReadTimeout:  3600 * time.Second,
			WriteTimeout: 3600 * time.Second,
		},
	}

	toReturn.server.Handler = toReturn.routes(apiKey, debug)

	return toReturn
}

// ListenAndServe starts serving requests on the endpoint specified.
func (s *HTTPDistributorServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

// Stop stops the server.
func (s *HTTPDistributorServer) Stop() {
	// TODO factor out the listener and close it here.
}

func (s *HTTPDistributorServer) routes(apiKey string, debug bool) http.Handler {
	r := httprouter.New()

	r.Handle(DistributorGetTasks.HTTPMethod, DistributorGetTasks.Path, makeHandler(s.handleGetTasks, apiKey, debug))

	r.Handle(DistributorPostTasks.HTTPMethod, DistributorPostTasks.Path, makeHandler(noResultHandler(s.handlePostTasks), apiKey, debug))

	r.Handle(DistributorPostHandIn.HTTPMethod, DistributorPostHandIn.Path, makeHandler(noResultHandler(s.handlePostHandIn), apiKey, debug))

	r.Handle(DistributorPostAnnounce.HTTPMethod, DistributorPostAnnounce.Path, makeHandler(s.handlePostAnnounce, apiKey, debug))

	r.Handle(DistributorGetQueues.HTTPMethod, DistributorGetQueues.Path, makeHandler(s.handleGetQueues, apiKey, debug))

	return r
}

// MakeHTTPAPIRequest makes a request to the HTTP API of the node at endpoint.
// The payload must be JSON serializable, the result must be a pointer to a
// struct ready for JSON deserialization.
// ErrUnknownMethod will be returned for an unknown apiMethod.
// ErrContentWithGet will be returned if payload != nil and apiMethod specifies
// GET as the HTTP verb.
func MakeHTTPAPIRequest(c *http.Client, endpoint Endpoint, apiMethod Method, params url.Values, payload interface{}, result interface{}) error {
	if params == nil {
		params = url.Values{}
	}

	if !isKnownMethod(apiMethod) {
		return ErrUnknownMethod
	}

	var (
		resp *http.Response
		err  error
	)

	u, err := url.Parse(fmt.Sprintf("http://%s:%d%s", endpoint.IP.String(), endpoint.Port, string(apiMethod.Path)))
	if err != nil {
		return errors.Wrap(err, "unable to build URL")
	}
	u.RawQuery = params.Encode()

	switch apiMethod.HTTPMethod {
	case http.MethodGet:
		if payload != nil {
			return ErrContentWithGet
		}
		resp, err = c.Get(u.String())
	case http.MethodPost:
		if payload != nil {
			bb := &bytes.Buffer{}
			err = json.NewEncoder(bb).Encode(payload)
			if err != nil {
				return errors.Wrap(err, "unable to encode payload")
			}
			resp, err = c.Post(u.String(), "application/json", bb)
		} else {
			resp, err = c.Post(u.String(), "", nil)
		}
	default:
		panic("API method " + string(apiMethod.Path) + " has invalid HTTP method")
	}

	if err != nil {
		return errors.Wrap(err, "unable to perform request")
	}

	// This can be nil for weird edge cases with network issues
	if resp != nil && resp.Body != nil {
		defer func() {

			err := resp.Body.Close()
			if err != nil {
				log.Println(err)
			}

		}()

		res := response{
			Ok:     false,
			Error:  "",
			Result: result,
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "unable to read HTTP response")
		}

		err = json.Unmarshal(b, &res)
		if err != nil {
			return errors.Wrap(err, "unable to parse response")
		}

		if !res.Ok {
			return errors.Wrap(errors.New(res.Error), "remote returned error")
		}
	}

	return nil
}
