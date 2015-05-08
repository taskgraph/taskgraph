package master

import (
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

func (m *master) NotifyWorker(ctx context.Context, workerID uint64, method string, input proto.Message) (proto.Message, error) {
	panic("")
}

func (m *master) Intercept(ctx context.Context, method string, input proto.Message) (proto.Message, error) {
	panic("")
}
