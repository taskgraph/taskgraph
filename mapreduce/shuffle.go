package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	pb "./proto"
	"../../taskgraph"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type shuffleTask struct {
	epoch            uint64
	taskID           uint64
	topo             taskgraph.Topology
	logger           *log.Logger
	framework        taskgraph.Framework
	config           map[string]string
	mapNum           uint64
	desReducerTaskId uint64
	preparedMapper   map[uint64]bool
	shuffleContainer map[string][]string
	mapperNum        uint64

	epochChange chan *shuffleEvent
	dataReady   chan *shuffleEvent
	metaReady   chan *shuffleEvent
	finished    chan *shuffleEvent
	exitChan    chan struct{}
}

type shuffleEvent struct {
	ctx    context.Context
	epoch  uint64
	fromID uint64
}

type shuffleEmit struct {
	Key   string   `json:"key"`
	Value []string `json:"value"`
}

type mapperEmitKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (sf *shuffleTask) Init(taskID uint64, framework taskgraph.Framework) {

	sf.epoch = framework.GetEpoch()
	sf.framework = framework
	sf.logger = log.New(os.Stdout, "\r\n", log.Ldate|log.Ltime|log.Lshortfile)
	// sf.logger.Println(sf.framework.GetTopology().GetNeighbors("Prefix", sf.epoch)))
	sf.mapNum = uint64(len(sf.framework.GetTopology().GetNeighbors("Prefix", sf.epoch)))
	
	// sf.mapNum = framework.GetMapperNum()
	sf.taskID = taskID
	sf.preparedMapper = make(map[uint64]bool)
	sf.shuffleContainer = make(map[string][]string)

	sf.epochChange = make(chan *shuffleEvent, 1)
	sf.metaReady = make(chan *shuffleEvent, 1)
	sf.dataReady = make(chan *shuffleEvent, 1)
	sf.finished = make(chan *shuffleEvent, 1)
	sf.exitChan = make(chan struct{}, 1)
	go sf.run()
}

func (sf *shuffleTask) run() {
	for {
		select {
		case ec := <-sf.epochChange:
			sf.doEnterEpoch(ec.ctx, ec.epoch)

		case shuffleDone := <-sf.finished:
			
			reducerID := sf.framework.GetTopology().GetNeighbors("Suffix", sf.epoch)[0]
			reducerPath := sf.framework.GetOutputContainerName() + "/reducer" + strconv.FormatUint(reducerID, 10)
			azureClient := sf.framework.GetAzureClient()
			shuffleWriteCloser, err := azureClient.OpenWriteCloser(reducerPath)
			if err != nil {
				sf.logger.Fatalf("MapReduce : Mapper read Error, ", err)
				return
			}
			for k := range sf.shuffleContainer {
				block := &shuffleEmit{
					Key:   k,
					Value: sf.shuffleContainer[k],
				}
				data, err := json.Marshal(block)
				if err != nil {
					sf.logger.Printf("Shuffle Emit json error, %v\n", err)
				}
				data = append(data, '\n')
				shuffleWriteCloser.Write(data)
			}
			sf.framework.FlagMeta(shuffleDone.ctx, "Prefix", "MetaReady")
			// sf.Exit()

		case metaMapperReady := <-sf.metaReady:

			sf.preparedMapper[metaMapperReady.fromID] = true
			
			if len(sf.preparedMapper) >= int(sf.mapNum) {
				sf.logger.Println(len(sf.preparedMapper), sf.mapNum)
				sf.framework.IncEpoch(metaMapperReady.ctx)
			}

		case <-sf.exitChan:
			return

		}
	}
}

func (sf *shuffleTask) EnterEpoch(ctx context.Context, epoch uint64) {
	sf.epochChange <- &shuffleEvent{ctx: ctx, epoch: epoch}
}

func (sf *shuffleTask) processKV(str []byte) {
	// tmpKV, err := strings.Split(str, " ")
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		sf.shuffleContainer[tp.Key] = append(sf.shuffleContainer[tp.Key], tp.Value)
	}
}

func (sf *shuffleTask) shuffleProgress(ctx context.Context) {
	azureClient := sf.framework.GetAzureClient()
	shufflePath := sf.framework.GetOutputContainerName() + "/shuffle" + strconv.FormatUint(sf.taskID-sf.mapNum, 10)
	shuffleReadCloser, err := azureClient.OpenReadCloser(shufflePath)
	if err != nil {
		sf.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReader(shuffleReadCloser)
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		
		if err != io.EOF && err != nil {
			sf.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			return
		}
		if err != io.EOF {
			str = str[:len(str) - 1]
		}
		sf.processKV(str)
	}
	sf.finished <- &shuffleEvent{ctx: ctx}

}

func (sf *shuffleTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	sf.logger.Printf("doEnterEpoch, Shuffle task %d, epoch %d", sf.taskID, epoch)
	sf.epoch = epoch
	if epoch == 1 {
		go sf.shuffleProgress(ctx)
	}
}

func (sf *shuffleTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	sf.metaReady <- &shuffleEvent{ctx: ctx, fromID: fromID}
}

func (sf *shuffleTask) CreateServer() *grpc.Server { 
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, sf)
	return server
}

func (sf *shuffleTask) CreateOutputMessage(method string) proto.Message { return nil }

func (sf *shuffleTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
}

func (sf *shuffleTask) Exit() {
	close(sf.exitChan)
}
