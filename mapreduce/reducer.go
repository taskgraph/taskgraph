import (
	"fmt"
	"io/ioutil"
	"log"
	"json"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type reducerTask struct {
	framework taskgraph.framework
	epoch uint64
	log *log.logger
	taskID uint64
	numOfTasks uint64
	preparedShuffle map[int]bool

	epochChange chan *event
	dataReady chan *event
	metaReady chan *event
	finished chan *event
	exitChan chan struct{}
}

type event struct {
	ctx context.Context
	epoch uint64
	fromID uint64
}

type shuffleEmit struct {
	Key string `json:"key"`
	Value []string `json:"value"`
}

func (rd *rducerTask) Init(taskId uint64, framework taskgraph.Framework) {
	rd.taskID = taskID
	rd.framework = framework
	rd.epoch = framework.GetEpoch()
	rd.shuffleNum = len(f.GetTopology().GetNeighbors("Prefix", rd.epoch))
	rd.preparedShuffle = make(map[int]bool)
	
	rd.epochChange = make(chan *event, 1)
	rd.metaReady = make(chan *event, 1)
	rd.dataReady = make(chan *event, 1)
	rd.finished = make(chan *event, 1)
	rd.exitChan = make(chan struct{}, 1)
}

func (rd *reducerTask) run {
	for {
		select {
			case ec := <-rd.epochChange:
				rd.doEnterEpoch(rd.ctx, rd.epoch)

			case reducerDone := <-rd.finished:
				rd.framework.ShutDownJob()

			case metaShuffleReady := <-rd.metaReady:
				preparedShuffle[metaShuffleReady.fromID] = true
				if (len(prepareShuffle) == rd.shuffleNum) {
					sf.framework.IncEopch(ctx)
				}

			case <-rd.exitChan:
				return

		}
	}
}

func (rd *reducerTask)  EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx : ctx, epoch : epoch}
}

func (rd *reducerTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	rd.logger.Prinntf("doEnterEpoch, Reducer task %d, epoch %d", t.taskID, epoch)
	rd.epoch = epoch
	if (epoch == 2)
		go rd.reducerProgress()
}

func (rd *reducerTask) reducerProgress() {
	reducerPath := rd.framework.GetOutputContainerName() + "/reducer" + strconv.Itoa(rd.taskID)
	azureClient := rd.framework.GetAzureClient()
	reducerReaderCloser, err := azureClient.openReaderCloser(reducerPath)
	if err != nil {
		rd.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReader(reducerReaderCloser)
	var str byte[]
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		str = str[:len(str) - 1]
		if err != io.EOF && err != nil {
			sf.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			return
		}
		rd.processKV(str)
	}
	rd.finished <- &event{ctx : ctx}
}

func (rd *reducerTask) processKV(str []byte]) {
	// tmpKV, err := strings.Split(str, " ") 
	var tp shuffleEmit
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
    	rd.framework.GetReducerFunc()(tp.Key, tp.Value)
    }
}

func (rd *reducerTask) MetaReady(ctx context.Context, fromID) {
	rd.metaReady <- &event{ctx : ctx, fromID : fromID}
}

func (rd *reducerTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {}

func (rd *reducerTask) Exit() {
	close(rd.exitChan)
}


