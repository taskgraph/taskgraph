package framework

import (
	"net/http"
	"time"

	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/framework/frameworkhttp"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"github.com/taskgraph/taskgraph/pkg/topoutil"
)

func (f *framework) sendRequest(dr *dataRequest) {
	addr, err := etcdutil.GetAddress(f.etcdClient, f.name, dr.taskID)
	if err != nil {
		// TODO: We should handle network faults later by retrying
		f.log.Fatalf("getAddress(%d) failed: %v", dr.taskID, err)
		return
	}

	if dr.retry {
		f.log.Printf("retry request data from task %d, addr %s", dr.taskID, addr)
	} else {
		f.log.Printf("request data from task %d, addr %s", dr.taskID, addr)
	}

	d, err := frameworkhttp.RequestData(addr, dr.req, f.taskID, dr.taskID, dr.epoch, f.log)
	// we need to retry if some task failed and there is a temporary Get request failure.
	if err != nil {
		f.log.Printf("RequestData from task %d (addr: %s) failed: %v", dr.taskID, addr, err)
		if err == frameworkhttp.ErrReqEpochMismatch {
			// It's out of date. Should wait for new epoch to set up.
			return
		}
		// Should retry for other errors.
		go func() {
			// we try again after the previous task key expires and hopefully another task
			// gets up and running.
			time.Sleep(2 * heartbeatInterval)
			dr.retry = true
			f.dataReqtoSendChan <- dr
		}()
		return
	}
	f.dataRespChan <- d
}

// This is used by the server side to handle data requests coming from remote.
func (f *framework) GetTaskData(taskID, epoch uint64, req string) ([]byte, error) {
	dataChan := make(chan []byte, 1)
	f.dataReqChan <- &dataRequest{
		taskID:   taskID,
		epoch:    epoch,
		req:      req,
		dataChan: dataChan,
	}

	select {
	case d, ok := <-dataChan:
		if !ok {
			// it assumes that only epoch mismatch will close the channel
			return nil, frameworkhttp.ErrReqEpochMismatch
		}
		return d, nil
	case <-f.httpStop:
		// If a node stopped running and there is remaining requests, we need to
		// respond error message back. It is used to let client routines stop blocking --
		// especially helpful in test cases.

		// This is used to drain the channel queue and get the rest notified.
		<-f.dataReqChan
		return nil, frameworkhttp.ErrServerClosed
	}
}

// Framework http server for data request.
// Each request will be in the format: "/datareq?taskID=XXX&req=XXX".
// "taskID" indicates the requesting task. "req" is the meta data for this request.
// On success, it should respond with requested data in http body.
func (f *framework) startHTTP() {
	f.log.Printf("serving http on %s\n", f.ln.Addr())
	// TODO: http server graceful shutdown
	handler := frameworkhttp.NewDataRequestHandler(f.log, f)
	err := http.Serve(f.ln, handler)
	select {
	case <-f.httpStop:
		f.log.Printf("http stops serving")
	default:
		if err != nil {
			f.log.Fatalf("http.Serve() returns error: %v\n", err)
		}
	}
}

// Close listener, stop HTTP server;
// Write error message back to under-serving responses.
func (f *framework) stopHTTP() {
	close(f.httpStop)
	f.ln.Close()
}

func (f *framework) sendResponse(dr *dataResponse) {
	dr.dataChan <- dr.data
}

func (f *framework) handleDataReq(dr *dataRequest) {
	// Note:
	// There are two improvement that I want to do:
	// 1. Make ServeAsX non-blocking API. This will require us to pass in
	//    a data channel.
	// 2. We can't leave a go-routine to wait for the data channel forever.
	//    Users might forget to close the channel if they didn't want to do anything.
	//	  I think we can clean it up in releaseEpochResource() as the epoch moves on.
	//    Because we won't be interested even though the data would come later.
	dataReceiver := make(chan []byte, 1)
	switch {
	case topoutil.IsParent(f.topology, dr.epoch, dr.taskID):
		f.task.ServeAsChild(dr.taskID, dr.req, dataReceiver)
	case topoutil.IsChild(f.topology, dr.epoch, dr.taskID):
		f.task.ServeAsParent(dr.taskID, dr.req, dataReceiver)
	default:
		f.log.Panic("unexpected")
	}
	go func() {
		select {
		case data, ok := <-dataReceiver:
			if !ok || data == nil {
				return
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
	}()
}

func (f *framework) handleDataResp(ctx taskgraph.Context, resp *frameworkhttp.DataResponse) {
	switch {
	case topoutil.IsParent(f.topology, resp.Epoch, resp.TaskID):
		f.task.ParentDataReady(ctx, resp.TaskID, resp.Req, resp.Data)
	case topoutil.IsChild(f.topology, resp.Epoch, resp.TaskID):
		f.task.ChildDataReady(ctx, resp.TaskID, resp.Req, resp.Data)
	default:
		f.log.Panic("unexpected")
	}
}
