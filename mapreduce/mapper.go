package mapreduce
import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)


type mapperTask struct {
	framework taskgraph.framework
	epoch uint64
	logger *log.logger
	taskId uint64
	config map[string][]string
	
	//channels
	epochChange chan *event
	// dataReady chan *event
	// metaReady chan *event
	fileUpdate chan int
	exitChan chan struct{}
}

struct event {
	ctx context.Context
	epoch uint64
}

func (mp *mapperTask) Init(taskID uint64, framework taskgraph.Framework) {
	mp.taskID = taskID
	mp.framework = framework
	mp.fileId = 0
	//channel init
	mp.epochChange = make(chan *event, 1)
	// mp.dataReady = make(chan *event, 1)
	// mp.metaReady = make(chan *event, 1)
	mp.exitChan = make(chan struct{})
	mp.fileUpdate = make(chan *event, 1)

	go mp.fileRead()
	go mp.run()
}

type event struct {
	ctx context.Context
	epoch uint64
	fromID uint64
	// response proto.Message
} 

func (mp *mapperTask) run() {
	for {
		select {
			case ec := <-mp.epochChange:
				mp.doEnterEpoch(mp.ctx, mp.epoch)

			case mapperDone := <-mp.fileUpdate:
				mp.framework.FlagMeta(mapperDone, "Suffix", "metaReady")
				mp.framework.ShutdownJob()

			case <-t.exitChan:
				return
		}
	}
}

func (mp *mapperTask) fileRead() {
	fileNum := len(mp.config["files"])
	files := mp.config["files"]
	for (i := 0; i < fileNum; fileID++) {
		mapperReaderCloser, err := mp.framework.GetAzureClient.OpenReadCloser(files[i])
		if err != nil {
			mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
			return
		}
		err = nil
		var str byte[]
		bufioReader := bufio.NewReader(mapperReaderCloser)
		for err != io.EOF {
			str, err = bufioReader.ReadBytes('\n')
			str = str[:len(str) - 1]
			if err != io.EOF && err != nil {
				mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
				return
			}
			mp.framework.GetMapperFunc()(str)
		}
	}
}

// At present, epoch is not a required parameter for mapper
// but it may be useful in the future
func (mp *mapperTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (mp *mapperTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	mp.logger.Printf("doEnterEpoch, Mapper task %d, epoch %d", t.taskID, epoch)
	mp.epoch = epoch
}

func (mp *mapperTask) Exit() {
	close(mp.exitChan)
}

func (mp *mapperTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {}

func (mp *mapperTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {}