package framework

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (f *framework) fetch(ctx context.Context, toID uint64, method string, input proto.Message, outputC chan<- proto.Message, opts ...grpc.CallOption) {
	epoch, ok := ctx.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in context: %v", ctx)
	}
	// check the request if epoch is stale
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
				f.log.Panicf("etcd getAddress(%d) failed: %v", toID, err)
			}
			f.log.Printf("connecting to task: %d, addr: %v", toID, addr)
			cc, err := grpc.Dial(addr)
			if err != nil {
				f.log.Printf("grpc.Dial(%s) failed: %v", addr, err)
				continue
			}
			f.log.Printf("requesting data from task %d", toID)
			reply = f.task.CreateOutputMessage(method)
			err = grpc.Invoke(ctx, method, input, reply, cc)
			if err == nil {
				cc.Close()
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
	// check epoch again for the response.
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

func (f *framework) Fetch(ctx context.Context, toID uint64, method string, input proto.Message, outputC chan<- proto.Message, opts ...grpc.CallOption) {
	go f.fetch(ctx, toID, method, input, outputC, opts...)
}

// Starts user implemented grpc Server.
func (f *framework) startGRPC() {
	f.log.Printf("serving grpc on %s\n", f.ln.Addr())
	err := f.task.CreateGRPCServer().Serve(f.ln)
	select {
	case <-f.rpcStop:
		f.log.Printf("grpc server stopped")
	default:
		if err != nil {
			f.log.Fatalf("grpc server returns error: %v\n", err)
		}
	}
}

// Close listener, stop HTTP server;
// Write error message back to under-serving responses.
func (f *framework) stopGRPC() {
	close(f.rpcStop)
	f.ln.Close()
}
