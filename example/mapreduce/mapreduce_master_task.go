package mapreduce

import (
	"log"
	"math"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph//taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const nonExistWork = math.MaxUint64

type masterTask struct {
	framework  taskgraph.Framework
	taskType   string
	epoch      uint64
	logger     *log.Logger
	taskID     uint64
	numOfTasks uint64

	config mapredeuceConfig

	//channels
	epochChange  chan *mapreduceEvent
	dataReady    chan *mapreduceEvent
	metaReady    chan *mapreduceEvent
	finishedChan chan *mapreduceEvent
	notifyChan   chan *mapreduceEvent
	exitChan     chan struct{}

	mapreduceConfig MapreduceConfig
}

func (t *masterTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.epochChange = make(chan *event, 1)
	t.getWork = make(chan *event, t.numOfTasks)
	t.dataReady = make(chan *event, t.numOfTasks)
	t.metaReady = make(chan *event, t.numOfTasks)
	t.workerDone = make(chan *event, 1)
	t.exitChan = make(chan *event)
	go t.run()
}

func (t *masterTask) run() {
	for {
		select {
		case grabWork := <-t.getWork:
			assignWork(grabWork)
		case <-t.exitChan:
			return

		}
	}
}

func (*masterTask) Exit() {
	close(t.exitChan)
}

func (*masterTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
}

func (*masterTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (t *bwmfTask) EnterEpoch(ctx context.Context, epoch uint64) {
	// TODO: This should be done in Init(). I will fix this once framework is also refactored.
	// NOTE: I'm assuming that task 0 won't die.
	if t.taskID == 0 {
		t.framework.FlagMeta(ctx, "Neighbors", "0")
	}
}

func (t *bwmfTask) CreateOutputMessage(method string) proto.Message {
	switch method {
	case "/proto.Master/GetWork":
		return new(pb.WorkConfigResponse)
	case "/proto.Master/GetTShard":
		return new(pb.WorkConfigResponse)
	}
	panic("")
}

func (t *masterTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterBlockDataServer(server, t)
	return server
}
