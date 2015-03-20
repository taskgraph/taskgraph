package framework

import (
	"strings"
	"time"

	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (f *framework) sendRequest(dr *dataRequest) {
	addr, err := etcdutil.GetAddress(f.etcdClient, f.name, dr.taskID)
	if err != nil {
		// TODO: We should handle network faults later by retrying
		f.log.Panicf("getAddress(%d) failed: %v", dr.taskID, err)
		return
	}

	if dr.retry {
		f.log.Printf("retry request data from task %d, addr %s", dr.taskID, addr)
	} else {
		f.log.Printf("request data from task %d, addr %s", dr.taskID, addr)
	}

	cc, err := grpc.Dial(addr, grpc.WithTimeout(heartbeatInterval))
	// we need to retry if some task failed and there is a temporary Get request failure.
	if err != nil {
		f.log.Printf("grpc.Dial from task %d (addr: %s) failed: %v", dr.taskID, addr, err)
		// Should retry for other errors.
		go f.retrySendRequest(dr)
		return
	}
	reply := f.task.CreateOutputMessage(dr.method)
	err = grpc.Invoke(dr.ctx, dr.method, dr.input, reply, cc)
	if err != nil {
		if strings.Contains(err.Error(), "server epoch mismatch") {
			// It's out of date. Should abort this data request.
			return
		}
		f.log.Printf("grpc.Invoke from task %d (addr: %s), method: %s, failed: %v", dr.taskID, addr, dr.method, err)
		go f.retrySendRequest(dr)
		return
	}
	f.dataRespChan <- &dataResponse{
		epoch:    dr.epoch,
		taskID:   dr.taskID,
		linkType: dr.linkType,
		input:    dr.input,
		output:   reply,
	}
}

func (f *framework) retrySendRequest(dr *dataRequest) {
	// we try again after the previous task key expires and hopefully another task
	// gets up and running.
	time.Sleep(2 * heartbeatInterval)
	dr.retry = true
	f.dataReqtoSendChan <- dr
}

// Framework http server for data request.
// Each request will be in the format: "/datareq?taskID=XXX&req=XXX".
// "taskID" indicates the requesting task. "req" is the meta data for this request.
// On success, it should respond with requested data in http body.
func (f *framework) startHTTP() {
	f.log.Printf("serving grpc on %s\n", f.ln.Addr())
	err := f.task.CreateServer().Serve(f.ln)
	select {
	case <-f.httpStop:
		f.log.Printf("grpc stops serving")
	default:
		if err != nil {
			f.log.Fatalf("grpc.Serve returns error: %v\n", err)
		}
	}
}

// Close listener, stop HTTP server;
// Write error message back to under-serving responses.
func (f *framework) stopHTTP() {
	close(f.httpStop)
	f.ln.Close()
}

func (f *framework) handleDataResp(ctx context.Context, resp *dataResponse) {
	f.task.DataReady(ctx, resp.taskID, resp.linkType, resp.input, resp.output)
}
