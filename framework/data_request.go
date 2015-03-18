package framework

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/framework/frameworkhttp"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"github.com/taskgraph/taskgraph/pkg/topoutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (f *framework) Fetch(ctx context.Context, toID uint64, method string, input proto.Message, outputC chan<- proto.Message, opts ...grpc.CallOption) {
	epoch, ok := ctx.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in context: %v", ctx)
	}

	epochMismatchC := make(chan struct{})
	epochCheckedC := make(chan struct{})
	f.dataReqtoSendChan <- &dataRequest{
		epoch:          epoch,
		epochMismatchC: epochMismatchC,
		epochCheckedC:  epochCheckedC,
	}

	var reply proto.Message
	select {
	case <-epochMismatchC:
		return
	case <-epochCheckedC:
		for {
			// establish grpc ClientConn
			addr, err := etcdutil.GetAddress(f.etcdClient, f.name, toID)
			if err != nil {
				// TODO: etcd client error handling
				f.log.Panicf("getAddress(%d) failed: %v", toID, err)
				return
			}
			f.log.Printf("reconnecting to addr: %v", addr)
			cc, err := grpc.Dial(addr)
			if err != nil {
				f.log.Panicf("grpc.Dial(%s) failed: %v", addr, err)
			}
			f.log.Printf("requesting data from task %d", toID)
			reply = f.task.CreateOutputMessage(method)
			err = grpc.Invoke(ctx, method, input, reply, cc)
			if err == nil {
				break
			}

			f.log.Printf("grpc.Invoke, method: %s, from task %d (addr: %s), failed: %v", method, toID, addr, err)
			if grpc.Code(err) == codes.Canceled {
				// New epoch has been set up.
				// It happens when the data has been retrieved successfully before
				// it crashed and then task restarts still doing the same thing.
				return
			}

			// we need to retry if task failure happened
			time.Sleep(2 * heartbeatInterval)
		}
	}

	epochMismatchC = make(chan struct{})
	epochCheckedC = make(chan struct{})
	f.dataRespChan <- &dataResponse{
		epoch:          epoch,
		epochMismatchC: epochMismatchC,
		epochCheckedC:  epochCheckedC,
	}
	select {
	case <-epochMismatchC:
		close(outputC)
	case <-epochCheckedC:
		outputC <- reply
	}
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
	case <-f.rpcStop:
		// If a node stopped running and there is remaining requests, we need to
		// respond error message back. It is used to let client routines stop blocking --
		// especially helpful in test cases.

		// This is used to drain the channel queue and get the rest notified.
		<-f.dataReqChan
		return nil, frameworkhttp.ErrServerClosed
	}
}

// Starts user implemented grpc Server.
func (f *framework) startGRPC() {
	f.log.Printf("serving grpc on %s\n", f.ln.Addr())
	err := f.rpcServer.Serve(f.ln)
	select {
	case <-f.rpcStop:
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
	close(f.rpcStop)
	f.ln.Close()
}

func (f *framework) sendResponse(dr *dataResponse) {
	dr.dataChan <- dr.data
}

func (f *framework) handleDataReq(dr *dataRequest) {
	dataReceiver := make(chan []byte, 1)
	go func() {
		switch {
		case topoutil.IsParent(f.topology, dr.epoch, dr.taskID):
			b, err := f.task.ServeAsChild(dr.taskID, dr.req)
			if err != nil {
				// TODO: We should handle network faults later by retrying
				f.log.Fatalf("ServeAsChild Error with id = %d, %v\n", dr.taskID, err)
			}
			dataReceiver <- b

		case topoutil.IsChild(f.topology, dr.epoch, dr.taskID):
			b, err := f.task.ServeAsParent(dr.taskID, dr.req)
			if err != nil {
				// TODO: We should handle network faults later by retrying
				f.log.Fatalf("ServeAsParent Error with id = %d, %v\n", dr.taskID, err)
			}
			dataReceiver <- b
		default:
			f.log.Panic("unexpected")
		}
	}()
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
		case <-f.epochPassed:
			// We can't leave a go-routine to wait for the data channel forever.
			// Users might forget to close the channel if they didn't want to do anything.
			// We can clean it up in releaseEpochResource() as the epoch moves on.
			// Because we won't be interested even though the data would come later.
		}
	}()
}

func (f *framework) handleDataResp(ctx context.Context, resp *frameworkhttp.DataResponse) {
	switch {
	case topoutil.IsParent(f.topology, resp.Epoch, resp.TaskID):
		// f.task.ParentDataReady(ctx, resp.TaskID, resp.Req, resp.Data)
	case topoutil.IsChild(f.topology, resp.Epoch, resp.TaskID):
		// f.task.ChildDataReady(ctx, resp.TaskID, resp.Req, resp.Data)
	default:
		f.log.Panic("unexpected")
	}
}
