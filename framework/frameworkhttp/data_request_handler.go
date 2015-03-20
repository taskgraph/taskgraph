package frameworkhttp

import (
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

var (
	ErrReqEpochMismatch error = errors.New("data request error: epoch mismatch")
	ErrServerClosed     error = errors.New("server has been closed")
)

const (
	DataRequestPrefix string = "/datareq"
	DataRequestTaskID string = "taskID"
	DataRequestReq    string = "req"
	DataRequestMethod string = "method"
	DataRequestEpoch  string = "epoch"
)

type DataGetter interface {
	GetTaskData(uint64, uint64, string, string) ([]byte, error)
}

type dataReqHandler struct {
	logger *log.Logger
	DataGetter
}

type DataResponse struct {
	TaskID uint64
	Epoch  uint64
	Method string
	Req    string
	Data   []byte
}

func NewDataRequestHandler(logger *log.Logger, dg DataGetter) http.Handler {
	return &dataReqHandler{
		logger:     logger,
		DataGetter: dg,
	}
}

func (h *dataReqHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != DataRequestPrefix {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	// parse url query
	q := r.URL.Query()
	fromIDStr := q.Get(DataRequestTaskID)
	fromID, err := strconv.ParseUint(fromIDStr, 0, 64)
	if err != nil {
		h.logger.Panic("Internal error: fromID couldn't be parsed")
	}
	epochStr := q.Get(DataRequestEpoch)
	epoch, err := strconv.ParseUint(epochStr, 0, 64)
	if err != nil {
		h.logger.Panic("Internal error: epoch couldn't be parsed")
	}
	req := q.Get(DataRequestReq)
	linkType := q.Get(DataRequestMethod)

	b, err := h.GetTaskData(fromID, epoch, linkType, req)
	if err != nil {
		if err == ErrReqEpochMismatch || err == ErrServerClosed {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		h.logger.Panic("unimplemented")
	}
	if _, err := w.Write(b); err != nil {
		log.Printf("http: response write failed: %v", err)
	}
}

func RequestData(addr string, linkType, req string, from, to, epoch uint64, logger *log.Logger) (*DataResponse, error) {
	u := url.URL{
		Scheme: "http",
		Host:   addr,
		Path:   DataRequestPrefix,
	}
	q := u.Query()
	q.Add(DataRequestTaskID, strconv.FormatUint(from, 10))
	q.Add(DataRequestReq, req)
	q.Add(DataRequestMethod, linkType)
	q.Add(DataRequestEpoch, strconv.FormatUint(epoch, 10))
	u.RawQuery = q.Encode()
	urlStr := u.String()
	// send request
	// pass the response to the awaiting event loop for data response
	resp, err := http.Get(urlStr)
	if err != nil {
		// The error could be caused because: 1. network failure; 2. We might have
		// sent request to failed server.
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusInternalServerError {
		logger.Fatalf("http: response code = %d, expect = %d", resp.StatusCode, 200)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Fatalf("http: ioutil.ReadAll(%v) returns error: %v", resp.Body, err)
	}
	if resp.StatusCode == http.StatusInternalServerError {
		logger.Printf("http error bytes: %s", string(data))
		if string(data) == ErrReqEpochMismatch.Error() {
			return nil, ErrReqEpochMismatch
		}
		if string(data) == ErrServerClosed.Error() {
			return nil, ErrServerClosed
		}
		logger.Fatalf("Unknown error: %v", data)
	}
	return &DataResponse{
		TaskID: to,
		Epoch:  epoch,
		Method: linkType,
		Req:    req,
		Data:   data,
	}, nil
}
