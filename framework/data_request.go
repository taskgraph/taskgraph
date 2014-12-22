package framework

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-distributed/meritop/pkg/topoutil"
)

const (
	DataRequestPrefix string = "/datareq"
	DataRequestTaskID string = "taskID"
	DataRequestReq    string = "req"
	DataRequestEpoch  string = "epoch"
)

type dataReqHandler struct {
	reqChan chan *dataRequest
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
	epochStr := q.Get(DataRequestEpoch)
	epoch, err := strconv.ParseUint(epochStr, 0, 64)
	if err != nil {
		panic("epoch string couldn't be parsed")
	}
	req := q.Get(DataRequestReq)

	dataChan := make(chan []byte, 1)
	h.reqChan <- &dataRequest{
		TaskID:   fromID,
		Epoch:    epoch,
		Req:      req,
		dataChan: dataChan,
	}

	b, ok := <-dataChan
	if !ok {
		panic("TODO: we need to back pressure for epoch discrepancy")
	}
	if _, err := w.Write(b); err != nil {
		log.Printf("http: response write failed: %v", err)
	}
}

func (f *framework) handleDataReq(dr *dataRequest) {
	var b []byte
	switch {
	case topoutil.IsParent(f.topology, dr.Epoch, dr.TaskID):
		b = f.task.ServeAsChild(dr.TaskID, dr.Req)
	case topoutil.IsChild(f.topology, dr.Epoch, dr.TaskID):
		b = f.task.ServeAsParent(dr.TaskID, dr.Req)
	default:
		panic("unimplemented")
	}
	// Getting the data from task could take a long time. We need to let
	// the response to send go through event loop again to check epoch.
	checkChan := make(chan bool, 1)
	f.dataRespToSendChan <- &dataRequest{
		Epoch:     dr.Epoch,
		checkChan: checkChan,
	}
	if check := <-checkChan; !check {
		close(dr.dataChan)
	}
	dr.dataChan <- b
}

func requestData(addr string, req string, from, to, epoch uint64, logger *log.Logger) *dataResponse {
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
		logger.Fatalf("http: get failed: %v", err)
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
	return &dataResponse{
		TaskID: to,
		Epoch:  epoch,
		Req:    req,
		Data:   data,
	}
}

func (f *framework) handleDataResp(resp *dataResponse) {
	switch {
	case topoutil.IsParent(f.topology, resp.Epoch, resp.TaskID):
		f.task.ParentDataReady(resp.TaskID, resp.Req, resp.Data)
	case topoutil.IsChild(f.topology, resp.Epoch, resp.TaskID):
		f.task.ChildDataReady(resp.TaskID, resp.Req, resp.Data)
	default:
		panic("unimplemented")
	}
}
