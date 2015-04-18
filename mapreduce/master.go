package mapreduce

import (
	"bufio"
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

type masterTask struct {
	framework  taskgraph.MapreduceFramework
	epoch      uint64
	logger     *log.Logger
	taskID     uint64
	numOfTasks uint64
	mapperNum  uint64
	shuffleNum uint64
	reducerNum uint64

	finishedMapper  map[uint64]bool
	finishedShuffle map[uint64]bool
	finishedReducer map[uint64]bool
	config          map[string]string

	epochChange chan *masterEvent
	dataReady   chan *masterEvent
	metaReady   chan *masterEvent
	finished    chan *masterEvent
	exitChan    chan struct{}
}

type masterEvent struct {
	ctx    context.Context
	epoch  uint64
	fromID uint64
}

func (m *masterTask) Init(taskID uint64, framework taskgraph.MapreduceFramework) {
	m.taskID = taskID
	m.framework = framework
	m.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	m.epoch = framework.GetEpoch()

	m.finishedMapper = make(map[uint64]bool)
	m.finishedShuffle = make(map[uint64]bool)
	m.finishedReducer = make(map[uint64]bool)

	m.epochChange = make(chan *masterEvent, 1)
	m.metaReady = make(chan *masterEvent, 1)
	m.dataReady = make(chan *masterEvent, 1)
	m.finished = make(chan *masterEvent, 1)
	m.exitChan = make(chan struct{}, 1)
	go m.run()
}

func (m *masterTask) run() {
	for {
		select {
		case ec := <-m.epochChange:
			m.doEnterEpoch(ec.ctx, ec.epoch)

		case metaReady := <-m.metaReady:
			m.finishedReducer[metaReady.fromID] = true
			m.processReducerOut(metaReady.fromID)

			if len(m.finishedReducer) >= int(m.reducerNum) {
				m.framework.ShutdownJob()
			}

		case <-m.exitChan:
			return

		}
	}
}

func (m *masterTask) processReducerOut(taskID uint64) {
	client := m.framework.GetClient()
	reducerPath := m.framework.GetOutputDirName() + "/" + "reducerResult" + strconv.FormatUint(taskID, 10)
	reducerReadCloser, err := client.OpenReadCloser(reducerPath)
	outputCloser, err := client.OpenWriteCloser(m.framework.GetOutputDirName() + "/" + m.framework.GetOutputFileName())
	if err != nil {
		m.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReader(reducerReadCloser)
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')

		if err != io.EOF && err != nil {
			m.logger.Fatalf("MapReduce : Master read Error, ", err)
			return
		}
		outputCloser.Write(str)
	}
	m.logger.Printf("%s removing..\n", reducerPath)
	err = client.Remove(reducerPath)
	if err != nil {
		m.logger.Fatal(err)
	}
}

func (m *masterTask) EnterEpoch(ctx context.Context, epoch uint64) {
	m.epochChange <- &masterEvent{ctx: ctx, epoch: epoch}
}

func (m *masterTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	m.logger.Printf("doEnterEpoch, Reducer task %d, epoch %d", m.taskID, epoch)
	m.epoch = epoch
}

func (m *masterTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	m.metaReady <- &masterEvent{ctx: ctx, fromID: fromID}
}

func (m *masterTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, m)
	return server
}

func (m *masterTask) CreateOutputMessage(method string) proto.Message { return nil }

func (m *masterTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (m *masterTask) Exit() {
	close(m.exitChan)
}
