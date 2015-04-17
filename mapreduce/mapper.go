package mapreduce

import (
	"bufio"
	"io"
	"log"
	"os"
	// "strconv"

	"github.com/golang/protobuf/proto"
	// "github.com/taskgraph/taskgraph/filesystem"
	pb "./proto"
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

	mp.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	mp.taskID = taskID
	mp.framework = framework
	

	//channel init
	mp.epochChange = make(chan *mapperEvent, 1)
	mp.dataReady = make(chan *mapperEvent, 1)
	mp.metaReady = make(chan *mapperEvent, 1)
	mp.exitChan = make(chan struct{})
	mp.fileUpdate = make(chan *mapperEvent, 1)

	
	go mp.run()
}

func (mp *mapperTask) run() {
	for {
		select {
		case ec := <-mp.epochChange:
			mp.doEnterEpoch(ec.ctx, ec.epoch)

		case mapperDone := <-mp.fileUpdate:
			mp.framework.FlagMeta(mapperDone.ctx, "Prefix", "metaReady")

		case <-mp.exitChan:
			return
		}
	}
}

func (mp *mapperTask) fileRead(ctx context.Context) {
	fileNum := len(mp.config["files"])
	mp.logger.Printf("fileReader, Mapper task %d %d", mp.taskID, fileNum)
	files := mp.config["files"]
	azureClient := mp.framework.GetAzureClient()
	for i := 0; i < fileNum; i++ {
		mp.logger.Printf("In loop %d", i)
		mp.logger.Println("In Azure Strage Client", files[i])
		mp.logger.Println(azureClient)

		mapperReaderCloser, err := azureClient.OpenReadCloser(files[i])
		mp.logger.Println(mapperReaderCloser)
		mp.logger.Println(azureClient)
		if err != nil {
			mp.logger.Panicf("MapReduce : get azure storage client reader failed, ", err)
			return
		}
		err = nil

		var str string
		bufioReader := bufio.NewReader(mapperReaderCloser)
		for err != io.EOF {
			str, err = bufioReader.ReadString('\n')

			if err != io.EOF && err != nil {
				mp.logger.Panicf("MapReduce : Mapper read Error, ", err)
				return
			}
			if err != io.EOF {
				str = str[:len(str)]
			}
			mapperFunc := mp.framework.GetMapperFunc()
			mp.logger.Println(str)
			mapperFunc(mp.framework, str)
		}
		mapperReaderCloser.Close()
	}
	mp.logger.Println("FileRead finished")
	mp.fileUpdate <- &mapperEvent{ctx : ctx, epoch : mp.epoch}
}

// At present, epoch is not a required parameter for mapper
// but it may be useful in the future
func (mp *mapperTask) EnterEpoch(ctx context.Context, epoch uint64) {
	mp.epochChange <- &mapperEvent{ctx: ctx, epoch: epoch}
}

func (mp *mapperTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	mp.logger.Printf("doEnterEpoch, Mapper task %d, epoch %d", mp.taskID, epoch)
	mp.epoch = epoch
	mp.logger.Printf("go fileReader, Mapper task %d", mp.taskID)
	mp.logger.Println(mp.config["files"][0])
	if epoch == 0 {
		go mp.fileRead(ctx)
	}

}

func (mp *mapperTask) Exit() {
	close(mp.exitChan)
}

func (mp *mapperTask) CreateServer() *grpc.Server { 
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, mp)
	return server
	
}

func (mp *mapperTask) CreateOutputMessage(method string) proto.Message { return nil }

func (mp *mapperTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (mp *mapperTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {}
