package frameworkhttp

import (
	"log"
	"net/http"
	"strconv"

	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/pkg/topoutil"
)

const (
	DataRequestPrefix string = "/datareq"
	DataRequestTaskID string = "taskID"
	DataRequestReq    string = "req"
)

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
