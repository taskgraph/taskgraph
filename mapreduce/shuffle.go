package mapreduce

import (
	"bufio"
	"encoding/json"
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

type shuffleTask struct {
	epoch            uint64
	taskID           uint64
	topo             taskgraph.Topology
	logger           *log.Logger
	framework        taskgraph.MapreduceFramework
	config           map[string]string
	mapNum           uint64
	preparedMapper   map[uint64]bool
	shuffleContainer map[string][]string

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

func (sf *shuffleTask) Init(taskID uint64, framework taskgraph.MapreduceFramework) {
	sf.epoch = framework.GetEpoch()
	sf.framework = framework
	sf.logger = log.New(os.Stdout, "\r\n", log.Ldate|log.Ltime|log.Lshortfile)
	sf.mapNum = uint64(len(sf.framework.GetTopology().GetNeighbors("Prefix", sf.epoch)))

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

func (sf *shuffleTask) transferShuffleData() {
	reducerPath := sf.framework.GetOutputDirName() + "/shuffle" + strconv.FormatUint(sf.taskID, 10)
	sf.framework.Clean(reducerPath)
	client := sf.framework.GetClient()
	shuffleWriteCloser, err := client.OpenWriteCloser(reducerPath)
	bufferWriter := bufio.NewWriterSize(shuffleWriteCloser, sf.framework.GetWriterBufferSize())
	if err != nil {
		sf.logger.Fatalf("MapReduce : Mapper read Error, ", err)
	}
	for k := range sf.shuffleContainer {
		block := &shuffleEmit{
			Key:   k,
			Value: sf.shuffleContainer[k],
		}
		data, err := json.Marshal(block)
		if err != nil {
			sf.logger.Fatalf("Shuffle Emit json error, %v\n", err)
		}
		data = append(data, '\n')
		bufferWriter.Write(data)
	}
	bufferWriter.Flush()
}

func (sf *shuffleTask) run() {
	for {
		select {
		case ec := <-sf.epochChange:
			sf.doEnterEpoch(ec.ctx, ec.epoch)

		case shuffleDone := <-sf.finished:
			sf.transferShuffleData()
			sf.framework.FlagMeta(shuffleDone.ctx, "Prefix", "MetaReady")

		case metaMapperReady := <-sf.metaReady:
			sf.logger.Printf("Meta Ready From Mapper %d", metaMapperReady.fromID)
			sf.preparedMapper[metaMapperReady.fromID] = true

			if len(sf.preparedMapper) >= int(sf.mapNum) {
				go sf.shuffleProgress(metaMapperReady.ctx)
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
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		sf.shuffleContainer[tp.Key] = append(sf.shuffleContainer[tp.Key], tp.Value)
	}
}

func (sf *shuffleTask) shuffleProgress(ctx context.Context) {
	var i uint64
	client := sf.framework.GetClient()
	for i = 0; i < sf.mapNum; i++ {	
		shufflePath := sf.framework.GetOutputDirName() + "/" + strconv.FormatUint(i, 10) + "mapper" + strconv.FormatUint(sf.taskID-sf.mapNum, 10)
		shuffleReadCloser, err := client.OpenReadCloser(shufflePath)
		if err != nil {
			sf.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		}
		bufioReader := bufio.NewReaderSize(shuffleReadCloser, sf.framework.GetReaderBufferSize())
		var str []byte
		err = nil
		for err != io.EOF {
			str, err = bufioReader.ReadBytes('\n')
			if err != io.EOF && err != nil {
				sf.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			}
			if err != io.EOF {
				str = str[:len(str)-1]
			}
			sf.processKV(str)
		}
		sf.logger.Printf("%s removing..\n", shufflePath)
		sf.framework.Clean(shufflePath)
	}
	sf.finished <- &shuffleEvent{ctx: ctx}

}

func (sf *shuffleTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	sf.logger.Printf("doEnterEpoch, Shuffle task %d, epoch %d", sf.taskID, epoch)
	sf.epoch = epoch
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
