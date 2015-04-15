package mapreduce

import (
	"bufio"
	"io"
	"log"

	"github.com/golang/protobuf/proto"
	// "github.com/taskgraph/taskgraph/filesystem"
	"../../taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type mapperTask struct {
	framework taskgraph.Framework
	epoch     uint64
	logger    *log.Logger
	taskID    uint64
	config    map[string][]string

	//channels
	epochChange chan *mapperEvent
	dataReady   chan *mapperEvent
	metaReady   chan *mapperEvent
	fileUpdate  chan *mapperEvent
	exitChan    chan struct{}
}

type mapperEvent struct {
	ctx    context.Context
	epoch  uint64
	fromID uint64
	// response proto.Message
}

func (mp *mapperTask) Init(taskID uint64, framework taskgraph.Framework) {
	mp.taskID = taskID
	mp.framework = framework

	//channel init
	mp.epochChange = make(chan *mapperEvent, 1)
	mp.dataReady = make(chan *mapperEvent, 1)
	mp.metaReady = make(chan *mapperEvent, 1)
	mp.exitChan = make(chan struct{})
	mp.fileUpdate = make(chan *mapperEvent, 1)

	go mp.fileRead()
	go mp.run()
}

func (mp *mapperTask) run() {
	for {
		select {
		case ec := <-mp.epochChange:
			mp.doEnterEpoch(ec.ctx, ec.epoch)

		case mapperDone := <-mp.fileUpdate:
			mp.framework.FlagMeta(mapperDone.ctx, "Suffix", "metaReady")
			mp.framework.ShutdownJob()

		case <-mp.exitChan:
			return
		}
	}
}

func (mp *mapperTask) fileRead() {
	fileNum := len(mp.config["files"])
	files := mp.config["files"]
	for i := 0; i < fileNum; i++ {
		azureClient := mp.framework.GetAzureClient()
		mapperReaderCloser, err := azureClient.OpenReadCloser(files[i])
		if err != nil {
			mp.logger.Fatalf("MapReduce : get azure storage client reader failed, ", err)
			return
		}
		err = nil
		var str string
		bufioReader := bufio.NewReader(mapperReaderCloser)
		for err != io.EOF {
			str, err = bufioReader.ReadString('\n')
			str = str[:len(str)-1]
			if err != io.EOF && err != nil {
				mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
				return
			}
			mapperFunc := mp.framework.GetMapperFunc()
			mapperFunc(mp.framework, str)
		}
	}
}

// At present, epoch is not a required parameter for mapper
// but it may be useful in the future
func (mp *mapperTask) EnterEpoch(ctx context.Context, epoch uint64) {
	mp.epochChange <- &mapperEvent{ctx: ctx, epoch: epoch}
}

func (mp *mapperTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	mp.logger.Printf("doEnterEpoch, Mapper task %d, epoch %d", mp.taskID, epoch)
	mp.epoch = epoch
}

func (mp *mapperTask) Exit() {
	close(mp.exitChan)
}

func (mp *mapperTask) CreateServer() *grpc.Server { return nil }

func (mp *mapperTask) CreateOutputMessage(method string) proto.Message { return nil }

func (mp *mapperTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (mp *mapperTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {}
