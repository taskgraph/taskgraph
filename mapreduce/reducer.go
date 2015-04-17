package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	pb "./proto"
	"../../taskgraph"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type reducerTask struct {
	framework       taskgraph.Framework
	epoch           uint64
	logger          *log.Logger
	taskID          uint64
	numOfTasks      uint64
	shuffleNum      uint64
	preparedShuffle map[uint64]bool
	config          map[string]string

	epochChange chan *reducerEvent
	dataReady   chan *reducerEvent
	metaReady   chan *reducerEvent
	finished    chan *reducerEvent
	exitChan    chan struct{}
}

type reducerEvent struct {
	ctx    context.Context
	epoch  uint64
	fromID uint64
}

func (rd *reducerTask) Init(taskID uint64, framework taskgraph.Framework) {
	rd.taskID = taskID
	rd.framework = framework
	rd.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	rd.epoch = framework.GetEpoch()
	rd.shuffleNum = uint64(len(framework.GetTopology().GetNeighbors("Prefix", rd.epoch)))
	rd.preparedShuffle = make(map[uint64]bool)

	rd.logger.Println("Reduce Task")

	rd.epochChange = make(chan *reducerEvent, 1)
	rd.metaReady = make(chan *reducerEvent, 1)
	rd.dataReady = make(chan *reducerEvent, 1)
	rd.finished = make(chan *reducerEvent, 1)
	rd.exitChan = make(chan struct{}, 1)
	go rd.run()
}

func (rd *reducerTask) run() {
	for {
		select {
		case ec := <-rd.epochChange:
			rd.doEnterEpoch(ec.ctx, ec.epoch)

		case reducerDone := <-rd.finished:
			rd.framework.FlagMeta(reducerDone.ctx, "Prefix", "metaReady")
			return

		case metaShuffleReady := <-rd.metaReady:
			rd.preparedShuffle[metaShuffleReady.fromID] = true
			if len(rd.preparedShuffle) == int(rd.shuffleNum) {
				// rd.chframework.IncEpoch(metaShuffleReady.ctx)
				go rd.reducerProgress(metaShuffleReady.ctx)
			}

		case <-rd.exitChan:
			return

		}
	}
}

func (rd *reducerTask) EnterEpoch(ctx context.Context, epoch uint64) {
	rd.epochChange <- &reducerEvent{ctx: ctx, epoch: epoch}
}

func (rd *reducerTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	rd.logger.Printf("doEnterEpoch, Reducer task %d, epoch %d", rd.taskID, epoch)
	rd.epoch = epoch
	// if epoch == 2 {
	// 	go rd.reducerProgress(ctx)
	// }
}

func (rd *reducerTask) reducerProgress(ctx context.Context) {
	reducerPath := rd.framework.GetOutputDirName() + "/reducer" + strconv.FormatUint(rd.taskID, 10)
	client := rd.framework.GetClient()
	reducerReadCloser, err := client.OpenReadCloser(reducerPath)
	rd.logger.Println("in reduce Progress")
	if err != nil {
		rd.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReader(reducerReadCloser)
	var str []byte 
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')

		if err != io.EOF && err != nil {
			rd.logger.Fatalf("MapReduce : Reducer read Error, ", err)
			return
		}

		if err != io.EOF {
			str = str[:len(str)]			
		}
		rd.processKV(str)
	}
	rd.logger.Printf("%s removing..\n", reducerPath)
	err = client.Remove(reducerPath)
	if err != nil {
		rd.logger.Fatal(err)
	}
	rd.finished <- &reducerEvent{ctx: ctx}
}

func (rd *reducerTask) processKV(str []byte) {
	// tmpKV, err := strings.Split(str, " ")
	var tp shuffleEmit
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		rd.framework.GetReducerFunc()(rd.framework, tp.Key, tp.Value)
	}
}

func (rd *reducerTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	rd.metaReady <- &reducerEvent{ctx: ctx, fromID: fromID}
}

func (rd *reducerTask) CreateServer() *grpc.Server { 
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, rd)
	return server
}

func (rd *reducerTask) CreateOutputMessage(method string) proto.Message { return nil }

func (rd *reducerTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (rd *reducerTask) Exit() {
	close(rd.exitChan)
}
