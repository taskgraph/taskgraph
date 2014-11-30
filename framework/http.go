package framework

import (
	"log"
	"net/http"
	"strconv"

	"github.com/go-distributed/meritop"
)

type dataReqHandler struct {
	topo  meritop.Topology
	task  meritop.Task
	epoch *uint64
}

func (h *dataReqHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != dataRequestPrefix {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	// parse url query
	q := r.URL.Query()
	fromIDStr := q.Get(dataRequestTaskID)
	fromID, err := strconv.ParseUint(fromIDStr, 0, 64)
	if err != nil {
		http.Error(w, "taskID couldn't be parsed", http.StatusBadRequest)
		return
	}
	req := q.Get(dataRequestReq)
	// ask task to serve data
	var b []byte
	switch {
	case isParent(h.topo, *h.epoch, fromID):
		b = h.task.ServeAsChild(fromID, req)
	case isChild(h.topo, *h.epoch, fromID):
		b = h.task.ServeAsParent(fromID, req)
	default:
		http.Error(w, "taskID isn't a parent or child of this task", http.StatusBadRequest)
		return
	}
	if _, err := w.Write(b); err != nil {
		log.Printf("http: response write errored: %v", err)
	}
}
