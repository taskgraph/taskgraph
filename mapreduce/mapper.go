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
	log *log.logger
	taskId uint64
	inputFile string
	config map[[]string]string
	
	fileId int
	//channels
	epochChange chan *event
	dataReady chan *event
	metaReady chan *event
	fileUpdate chan int
	exitChan chan struct{}
}

func (mp *mapperTask) Init(taskId uint64, framework taskgraph.Framework) {
	mp.taskID = taskID
	mp.framework = framework
	mp.fileId = 0
	//channel init
	mp.epochChange = make(chan *event, 1)
	mp.dataReady = make(chan *event, 1)
	mp.metaReady = make(chan *event, 1)
	mp.exitChan = make(chan struct{})
	mp.fileUpdate = make(chan *event, 1)

	go mp.fileRead()
	go mp.run()
}

type event struct {
	ctx context.Context
	epoch uint64
	fromID uint64
	response proto.Message
} 

func (mp *mapperTask) run() {
	for {
		select {
			case ec := <-mp.epochChange
			mp.doEnterEpoch(mp.ctx, mp.epoch)
			case mapperDone := <-mp.fileUpdate
			mp.framework.FlagMeta(mapperDone, "Suffix", "metaReady")
		}
	}
}

func (mp *mapperTask) fileRead() {
	var 
	for (fileID := 0; fileID < )
}

func (mp *bwmfTask) doEnterEpoch(ctx context.Context, epoch uint64) {

	mp.epoch = epoch

	
	// var method string

	// for _, c := range bt.framework.GetTopology().GetNeighbors("Neighbors", epoch) {
	// 	bt.logger.Println("Sending request ", method, " to neighbor [", c, "] at epoch ", epoch)
	// 	bt.framework.DataRequest(ctx, c, method, &pb.Request{Epoch: epoch})
	// }
}

func (mp *mapperTask) Exit() {
	close(mp.exitChan)
}

func (mp *mapperTask) DataReady() {}

func (mp *mapperTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	switch
}