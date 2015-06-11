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
	getWork		chan int
	finishedChan chan *mapreduceEvent
	notifyChanArr   []chan bool
	exitChan     chan struct{}

	mapreduceConfig MapreduceConfig
}

type mapreduceEvent {
	ctx context.Context
	fromID uint64
	linkType string
	method string
	meta string
	output proto.Message
}

func (t *masterTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.epochChange = make(chan *event, 1)
	t.getWork = make(chan *event, t.numOfTasks)
	t.dataReady = make(chan *event, t.numOfTasks)
	t.metaReady = make(chan *event, t.numOfTasks)
	t.notifyChanArr = make([]chan *event, t.numOfTasks)
	for i := range t.notifyChanArr {
		t.notifyChanArr[i] = make(bool, 1)

	}
	t.workerDone = make(chan *event, 1)
	t.exitChan = make(chan *event)
	go t.run()
}

func (t *masterTask) run() {
	for {
		select {
		case requestWorker := <-t.getWork:
			t.assignWork(requestWorker)
		case metaReady := <-t.metaReady:
			go t.processMessage(metaReady.ctx, metaReady.fromID, metaReady.linkType, metaReady.meta)
		case <-t.exitChan:
			return

		}
	}
}

func (t *masterTask) GetWork(in *WorkRequest) (*WorkConfigResponse, error) {
	t.getWork<-in.taskID
}

func (t *masterTask) assignWork() {

}


func (mp *mapreduceTask) processMessage(ctx context.Context, fromID uint64, linkType string, meta string) {
	switch mp.taskType {
	case "master":
		matchMapper, _ := regexp.MatchString("^MapperWorkFinished[0-9]+$", meta)
		matchReducer, _ := regexp.MatchString("^ReducerWorkFinished[0-9]+$", meta)
		switch {
		case matchMapper:
			mp.mapperNumCount++
			mp.logger.Printf("==== finished %d works, total %d works, receive meta %s====", mp.mapperNumCount, mp.mapperWorkNum, meta)
			if mp.mapperWorkNum <= mp.mapperNumCount {
				mp.framework.IncEpoch(ctx)
			}
		case matchReducer:
			mp.reducerNumCount++
			mp.logger.Printf("==== finished %d works, total %d works, receive meta %s====", mp.reducerNumCount, mp.mapreduceConfig.ReducerNum, meta)
			if mp.mapreduceConfig.ReducerNum <= mp.reducerNumCount {
				mp.framework.ShutdownJob()
			}
		}

	}
}



func (*masterTask) Exit() {
	close(t.exitChan)
}

func (t *masterTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	t.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}

func (*masterTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {

}

func (t *masterTask) initializeEnv() {
	t.workNum = // get workNum through etcd
	t.currentWork = // get current Work through etcd
}

func (t *masterTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.initializeEnv()
}

func (t *bwmfTask) CreateOutputMessage(method string) proto.Message {
	switch method {
	case "/proto.Master/GetWork":
		return new(pb.WorkConfigResponse)
	}
	panic("")
}

func (t *masterTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterBlockDataServer(server, t)
	return server
}
