package framework

import (
	"fmt"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	ErrEpochMismatch = fmt.Errorf("server epoch mismatch")
)

func (f *framework) CheckGRPCContext(ctx context.Context) error {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		return fmt.Errorf("Can't get grpc.Metadata from context: %v", ctx)
	}
	epoch, err := strconv.ParseUint(md["epoch"], 10, 64)
	if err != nil {
		return err
	}
	// send it to framework central select and check epoch.
	resChan := make(chan bool, 1)
	// The select loop might stop running but we still need to return error to user.
	// We can't use the ctx here because it's the grpc context.
	select {
	case f.epochCheckChan <- &epochCheck{
		epoch:   epoch,
		resChan: resChan,
	}:
	case <-f.globalStop:
		return fmt.Errorf("framework stopped")
	}
	ok = <-resChan
	if ok {
		return nil
	} else {
		return ErrEpochMismatch
	}
}

func (f *framework) DataRequest(ctx context.Context, toID uint64, method string, input proto.Message) {
	epoch, ok := ctx.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey or cast is in DataRequest")
	}
	// assumption here:
	// Event driven task will call this in a synchronous way so that
	// the epoch won't change at the time task sending this request.
	// Epoch may change, however, before the request is actually being sent.
	select {
	case f.dataReqtoSendChan <- &dataRequest{
		ctx:    f.makeGRPCContext(ctx),
		taskID: toID,
		epoch:  epoch,
		input:  input,
		method: method,
	}:
	case <-ctx.Done():
		f.log.Printf("abort data request, to %d, epoch %d, method %s", toID, epoch, method)
	}
}

// encode metadata to context in grpc specific way
func (f *framework) makeGRPCContext(ctx context.Context) context.Context {
	md := metadata.MD{
		"taskID": strconv.FormatUint(f.taskID, 10),
		"epoch":  strconv.FormatUint(f.epoch, 10),
	}
	return metadata.NewContext(ctx, md)
}

func (f *framework) sendRequest(dr *dataRequest) {
	addr, err := etcdutil.GetAddress(f.etcdClient, f.name, dr.taskID)
	if err != nil {
		f.log.Printf("getAddress(%d) failed: %v", dr.taskID, err)
		go f.retrySendRequest(dr)
		return
	}
	// TODO: reuse ClientConn and reply message.
	// The grpc.WithTimeout would help detect any disconnection in failfast.
	// Otherwise grpc.Invoke will keep retrying.
	cc, err := grpc.Dial(addr, grpc.WithTimeout(heartbeatInterval))
	// we need to retry if some task failed and there is a temporary Get request failure.
	if err != nil {
		f.log.Printf("grpc.Dial to task %d (addr: %s) failed: %v", dr.taskID, addr, err)
		// Should retry for other errors.
		go f.retrySendRequest(dr)
		return
	}
	defer cc.Close()
	if dr.retry {
		f.log.Printf("retry data request %s to task %d, addr %s", dr.method, dr.taskID, addr)
	} else {
		f.log.Printf("data request %s to task %d, addr %s", dr.method, dr.taskID, addr)
	}
	reply := f.task.CreateOutputMessage(dr.method)
	err = grpc.Invoke(dr.ctx, dr.method, dr.input, reply, cc)
	if err != nil {
		f.log.Printf("grpc.Invoke to task %d (addr: %s), method: %s, failed: %v", dr.taskID, addr, dr.method, err)
		go f.retrySendRequest(dr)
		return
	}

	select {
	case f.dataRespChan <- &dataResponse{
		epoch:  dr.epoch,
		taskID: dr.taskID,
		method: dr.method,
		input:  dr.input,
		output: reply,
	}:
	case <-dr.ctx.Done():
		f.log.Printf("abort data response, to %d, epoch %d, method %s", dr.taskID, dr.epoch, dr.method)
	}
}

func (f *framework) retrySendRequest(dr *dataRequest) {
	// we try again after the previous task key expires and hopefully another task
	// gets up and running.
	time.Sleep(2 * heartbeatInterval)
	dr.retry = true
	select {
	case f.dataReqtoSendChan <- dr:
	case <-dr.ctx.Done():
		f.log.Printf("abort data request, to %d, epoch %d, method %s", dr.taskID, dr.epoch, dr.method)
	}
}

// Framework http server for data request.
// Each request will be in the format: "/datareq?taskID=XXX&req=XXX".
// "taskID" indicates the requesting task. "req" is the meta data for this request.
// On success, it should respond with requested data in http body.
func (f *framework) startHTTP() {
	f.log.Printf("serving grpc on %s\n", f.ln.Addr())
	server := f.task.CreateServer()
	err := server.Serve(f.ln)
	select {
	case <-f.globalStop:
		server.Stop()
		f.log.Printf("grpc stops serving")
	default:
		if err != nil {
			f.log.Fatalf("grpc.Serve returns error: %v\n", err)
		}
	}
}

func (f *framework) handleDataResp(ctx context.Context, resp *dataResponse) {
	f.task.DataReady(ctx, resp.taskID, resp.method, resp.output)
}
