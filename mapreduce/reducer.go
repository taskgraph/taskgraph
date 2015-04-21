package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"

	"../../taskgraph"
	pb "./proto"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type reducerTask struct {
	framework       taskgraph.MapreduceFramework
	epoch           uint64
	logger          *log.Logger
	taskID          uint64
	numOfTasks      uint64
	shuffleNum      uint64
	preparedShuffle map[uint64]bool
	config          map[string]string
	reducerFunc func(taskgraph.MapreduceFramework, string, []string)

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

func (rd *reducerTask) Init(taskID uint64, framework taskgraph.MapreduceFramework) {
	rd.taskID = taskID
	rd.framework = framework
	rd.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	rd.epoch = framework.GetEpoch()
	rd.shuffleNum = uint64(len(framework.GetTopology().GetNeighbors("Prefix", rd.epoch)))
	rd.preparedShuffle = make(map[uint64]bool)
	rd.reducerFunc = rd.framework.GetReducerFunc()
	rd.logger.Println("Reduce Task")
	rd.framework.SetReducerOutputWriter()

	// channel init
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

		case metaShuffleReady := <-rd.metaReady:
			rd.logger.Printf("Meta Ready From Shuffle %d", metaShuffleReady.fromID)
			rd.preparedShuffle[metaShuffleReady.fromID] = true
			rd.reducerProgress(metaShuffleReady.fromID)
			if len(rd.preparedShuffle) == int(rd.shuffleNum) {
				rd.framework.FinishReducer()
				rd.finished <- &reducerEvent{ctx: metaShuffleReady.ctx}
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
}

func (rd *reducerTask) reducerProgress(fromID uint64) {
	reducerPath := rd.framework.GetOutputDirName() + "/shuffle" + strconv.FormatUint(fromID, 10)
	client := rd.framework.GetClient()
	reducerReadCloser, err := client.OpenReadCloser(reducerPath)
	if err != nil {
		rd.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReaderSize(reducerReadCloser, rd.framework.GetReaderBufferSize())
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		if err != io.EOF && err != nil {
			rd.logger.Fatalf("MapReduce : Reducer read Error, ", err)
			return
		}
		if err != io.EOF {
			str = str[:len(str) - 1]
		}
		rd.processKV(str)
	}
	rd.logger.Printf("%s removing..\n", reducerPath)
	rd.framework.Clean(reducerPath)
}

func (rd *reducerTask) processKV(str []byte) {
	var tp shuffleEmit
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		rd.reducerFunc(rd.framework, tp.Key, tp.Value)
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
