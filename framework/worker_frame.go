package framework

import (
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

func (w *worker) NotifyMaster(ctx context.Context, method string, input proto.Message) (proto.Message, error) {
	panic("")
}

func (w *worker) DataRequest(ctx context.Context, workerID uint64, method string, input proto.Message) (proto.Message, error) {
	panic("")
}

func (w *worker) Intercept(ctx context.Context, method string, input proto.Message) (proto.Message, error) {
	panic("")
}
