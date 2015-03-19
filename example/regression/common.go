package regression

import (
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
)

type taskCommon struct {
	epoch        uint64
	taskID       uint64
	framework    taskgraph.Framework
	logger       *log.Logger
	param        *pb.Parameter
	gradient     *pb.Gradient
	fromChildren map[uint64]*pb.Gradient
	NodeProducer chan bool
	config       map[string]string
}

func (t *taskCommon) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func (t *taskCommon) Exit() {}

func (t *taskCommon) CreateOutputMessage(methodName string) proto.Message {
	switch methodName {
	case "/proto.Regression/GetParameter":
		return new(pb.Parameter)
	case "/proto.Regression/GetGradient":
		return new(pb.Gradient)
	default:
		t.logger.Panicf("Unknown method: %s", methodName)
		return nil
	}
}

func probablyFail(levelStr string) bool {
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return false
	}
	if level < rand.Intn(100)+1 {
		return false
	}
	return true
}
