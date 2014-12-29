package framework

import (
	"net/http"

	"github.com/go-distributed/meritop/framework/frameworkhttp"
	"github.com/go-distributed/meritop/pkg/etcdutil"
	"github.com/go-distributed/meritop/pkg/topoutil"
)

func (f *framework) sendRequest(dr *dataRequest) {
	addr, err := etcdutil.GetAddress(f.etcdClient, f.name, dr.taskID)
	if err != nil {
		// TODO: We should handle network faults later by retrying
		f.log.Fatalf("getAddress(%d) failed: %v", dr.taskID, err)
		return
	}
	d, err := frameworkhttp.RequestData(addr, dr.req, f.taskID, dr.taskID, dr.epoch, f.log)
	if err != nil {
		if err != frameworkhttp.EpochMismatchError {
			f.log.Printf("Epoch mismatch error from server")
			return
		}
		f.log.Printf("RequestData failed: %v", err)
		return
	}
	f.dataRespChan <- d
}

func (f *framework) GetTaskData(taskID, epoch uint64, req string) ([]byte, error) {
	dataChan := make(chan []byte, 1)
	f.dataReqChan <- &dataRequest{
		taskID:   taskID,
		epoch:    epoch,
		req:      req,
		dataChan: dataChan,
	}

	d, ok := <-dataChan
	if !ok {
		// it assumes that only epoch mismatch will close the channel
		return nil, frameworkhttp.EpochMismatchError
	}
	return d, nil
}

// Framework http server for data request.
// Each request will be in the format: "/datareq?taskID=XXX&req=XXX".
// "taskID" indicates the requesting task. "req" is the meta data for this request.
// On success, it should respond with requested data in http body.
func (f *framework) startHTTP() {
	f.log.Printf("task %d serving http on %s\n", f.taskID, f.ln.Addr())
	// TODO: http server graceful shutdown
	handler := frameworkhttp.NewDataRequestHandler(f.log, f)
	if err := http.Serve(f.ln, handler); err != nil {
		f.log.Fatalf("http.Serve() returns error: %v\n", err)
	}
}

func (f *framework) sendResponse(dr *dataResponse) {
	dr.dataChan <- dr.data
}

func (f *framework) handleDataReq(dr *dataRequest) {
	var data []byte
	switch {
	case topoutil.IsParent(f.topology, dr.epoch, dr.taskID):
		data = f.task.ServeAsChild(dr.taskID, dr.req)
	case topoutil.IsChild(f.topology, dr.epoch, dr.taskID):
		data = f.task.ServeAsParent(dr.taskID, dr.req)
	default:
		f.log.Panic("unexpected")
	}
	// Getting the data from task could take a long time. We need to let
	// the response-to-send go through event loop to check epoch.
	f.dataRespToSendChan <- &dataResponse{
		taskID:   dr.taskID,
		epoch:    dr.epoch,
		req:      dr.req,
		data:     data,
		dataChan: dr.dataChan,
	}
}

func (f *framework) handleDataResp(resp *frameworkhttp.DataResponse) {
	switch {
	case topoutil.IsParent(f.topology, resp.Epoch, resp.TaskID):
		f.task.ParentDataReady(resp.TaskID, resp.Req, resp.Data)
	case topoutil.IsChild(f.topology, resp.Epoch, resp.TaskID):
		f.task.ChildDataReady(resp.TaskID, resp.Req, resp.Data)
	default:
		f.log.Panic("unexpected")
	}
}
