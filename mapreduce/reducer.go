package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"strconv"

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
	rd.epoch = framework.GetEpoch()
	rd.shuffleNum = uint64(len(framework.GetTopology().GetNeighbors("Prefix", rd.epoch)))
	rd.preparedShuffle = make(map[uint64]bool)

	rd.epochChange = make(chan *reducerEvent, 1)
	rd.metaReady = make(chan *reducerEvent, 1)
	rd.dataReady = make(chan *reducerEvent, 1)
	rd.finished = make(chan *reducerEvent, 1)
	rd.exitChan = make(chan struct{}, 1)
}

func (rd *reducerTask) run() {
	for {
		select {
		case ec := <-rd.epochChange:
			rd.doEnterEpoch(ec.ctx, ec.epoch)

		case <-rd.finished:
			rd.framework.ShutdownJob()

		case metaShuffleReady := <-rd.metaReady:
			rd.preparedShuffle[metaShuffleReady.fromID] = true
			if len(rd.preparedShuffle) == int(rd.shuffleNum) {
				rd.framework.IncEpoch(metaShuffleReady.ctx)
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
	if epoch == 2 {
		go rd.reducerProgress(ctx)
	}
}

func (rd *reducerTask) reducerProgress(ctx context.Context) {
	reducerPath := rd.framework.GetOutputContainerName() + "/reducer" + strconv.FormatUint(rd.taskID, 10)
	azureClient := rd.framework.GetAzureClient()
	reducerReadCloser, err := azureClient.OpenReadCloser(reducerPath)
	if err != nil {
		rd.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReader(reducerReadCloser)
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		str = str[:len(str)-1]
		if err != io.EOF && err != nil {
			rd.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			return
		}
		rd.processKV(str)
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

func (rd *reducerTask) CreateServer() *grpc.Server { return nil }

func (rd *reducerTask) CreateOutputMessage(method string) proto.Message { return nil }

func (rd *reducerTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (rd *reducerTask) Exit() {
	close(rd.exitChan)
}
