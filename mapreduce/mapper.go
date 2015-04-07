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
	numOfTasks uint64
	inputFile string

	//channels
	epochChange chan *event
	dataReady chan *event
	metaReady chan *event
	exitChan chan struct{}
}


func (mp *mapperTask) Init(taskId uint64, framework taskgraph.Framework) {
	mp.taskID = taskID
	mp.framework = framework
	mp.inputFile = 

	//channel init
	mp.epochChange = make(chan *event, 1)
	mp.dataReady = make(chan *event, 1)
	mp.metaReady = make(chan *event, 1)
	mp.exitChan = make(chan struct{})

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
		switch {

		}
	}
}

func (mp *mapperTask) Exit() {
	close(mp.exitChan)
}

func (mp *mapperTask) DataReady() {}

func (mp *mapperTask) 