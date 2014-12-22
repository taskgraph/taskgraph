package frameworkhttp

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/pkg/topoutil"
)

const (
	DataRequestPrefix string = "/datareq"
	DataRequestTaskID string = "taskID"
	DataRequestReq    string = "req"
	DataRequestEpoch  string = "epoch"
)

type DataResponse struct {
	TaskID uint64
	Req    string
	Data   []byte
}

type dataReqHandler struct {
	topo    meritop.Topology
	task    meritop.Task
	epocher Epocher
}

func NewDataRequestHandler(topo meritop.Topology, task meritop.Task, epocher Epocher) http.Handler {
	return &dataReqHandler{
		topo:    topo,
		task:    task,
		epocher: epocher,
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
		http.Error(w, "taskID couldn't be parsed", http.StatusBadRequest)
		return
	}
	req := q.Get(DataRequestReq)
	// ask task to serve data
	var b []byte
	switch {
	case topoutil.IsParent(h.topo, h.epocher.GetEpoch(), fromID):
		b = h.task.ServeAsChild(fromID, req)
	case topoutil.IsChild(h.topo, h.epocher.GetEpoch(), fromID):
		b = h.task.ServeAsParent(fromID, req)
	default:
		http.Error(w, "taskID isn't a parent or child of this task", http.StatusBadRequest)
		return
	}
	if _, err := w.Write(b); err != nil {
		log.Printf("http: response write errored: %v", err)
	}
}

func RequestData(addr string, req string, from, to, epoch uint64, logger *log.Logger) *DataResponse {
	u := url.URL{
		Scheme: "http",
		Host:   addr,
		Path:   DataRequestPrefix,
	}
	q := u.Query()
	q.Add(DataRequestTaskID, strconv.FormatUint(from, 10))
	q.Add(DataRequestReq, req)
	q.Add(DataRequestEpoch, strconv.FormatUint(epoch, 10))
	u.RawQuery = q.Encode()
	urlStr := u.String()
	// send request
	// pass the response to the awaiting event loop for data response
	resp, err := http.Get(urlStr)
	if err != nil {
		logger.Fatalf("http: get(%s) returns error: %v", urlStr, err)
	}
	defer resp.Body.Close()
	// TODO: we need to handle epoch discrepancy response
	if resp.StatusCode != 200 {
		logger.Fatalf("http: response code = %d, expect = %d", resp.StatusCode, 200)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Fatalf("http: ioutil.ReadAll(%v) returns error: %v", resp.Body, err)
	}
	return &DataResponse{
		TaskID: to,
		Req:    req,
		Data:   data,
	}
}
