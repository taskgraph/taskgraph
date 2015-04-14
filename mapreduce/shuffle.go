import (
	"fmt"
	"io/ioutil"
	"log"
	"json"
	"os"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type shuffleTask struct {
	epoch uint64
	taskID uint64
	topo taskgraph.topo
	framework taskgraph.framework
	numOfMapper uint64
	desReducerTaskId uint64
	preparedMapper map[int]bool
	shuffleContainer map[string][]string
	mapperNum uint64

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

type mapperEmitKV struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

func (sf *shuffleTask) Init(taskID uint64, framework taskgraph.Framework) {
	
	sf.epoch = framework.GetEpoch()
	sf.mapNum = len(f.GetTopology().GetNeighbors("Prefix", sf.epoch))
	// sf.mapNum = framework.GetMapperNum()
	sf.taskID = taskID
	sf.preparedMapper = make(map[int]bool)
	sf.shuffleContainer = make(map[string][]string)

	sf.epochChange = make(chan *event, 1)
	sf.metaReady = make(chan *event, 1)
	sf.dataReady = make(chan *event, 1)
	sf.finished = make(chan *event, 1)
	sf.exitChan = make(chan struct{}, 1)
	go sf.run()
}

func (sf *shuffleTask) run {
	for {
		select {
			case ec := <-sf.epochChange:
				sf.doEnterEpoch(sf.ctx, sf.epoch)

			case shuffleDone := <-sf.finished:
				sf.framework.FlagMeta(mapperDone, "Suffix", "MetaReady")
				reducerID := f.GetTopology().GetNeighbors("Suffix", sf.epoch)[0]
				reducerPath := sf.framework.GetOutputContainerName() + "/reducer" + strconv.Itoa(reducerID)
				azureClient := sf.framework.GetAzureClient()
				shuffleWriterCloser, err := azureClient.openWriterCloser(reducerPath)
				for k := range sf.shuffleContianer {
					block := &shuffleEmit {
						Key : k,
						Value : sf.shuffleContianer[k]
					}
					data, err := json.Marshal(key)
					if (err) {
						sf.logger.Printf("Shuffle Emit json error, %v\n", err)
					}
					data = append(data, '\n')
					shuffleWriterCloser.write(data)
				}
				sf.framework.ShutdownJob()

			case metaMapperReady := <-metaReady:
				prepareMapper[metaMapperReady.fromID] = true
				if (len(prepareMapper) == sf.mapNum) {
					sf.framework.IncEpoch(ctx)
					
				}

			case <-sf.exitChan:
				return	
			
		}
	}
}


func (sf *shuffleTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx : ctx, epoch: epoch}
}

func (sf *shuffleTask) processKV(str []byte]) {
	// tmpKV, err := strings.Split(str, " ") 
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
    	sf.shuffleContainer[tp.Key].append(tp.Value)    
    }
}

func (sf *shuffleTask) shuffleProgress() {
	azureClient := sf.framework.GetAzureClient()
	shufflePath := sf.framework.GetOutputContainerName() + "/shuffle" + strconv.Itoa(sf.taksID - sf.mapNum)
	shuffleReaderCloser, err := azureClient.OpenReaderCloser(shufflePath)
	if err != nil {
		sf.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReader(shuffleReaderCloser)
	var str byte[]
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		str = str[:len(str) - 1]
		if err != io.EOF && err != nil {
			sf.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			return
		}
		sf.processKV(str)
	}
	sf.finished <- &event{ctx : ctx}
	

}

func (sf *shuffleTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	sf.logger.Printf("doEnterEpoch, Shuffle task %d, epoch %d", t.taskID, epoch)
	sf.epoch = epoch
	if (epoch == 1) 
		go sf.shuffleProgress()
}


func (sf *shuffleTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	sf.metaReady <- &event{ctx : ctx, fromID : fromID}
}

func (sf *shuffleTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {}

func (sf *shuffleTask) Exit() {
	close(sf.exitChan)
}

