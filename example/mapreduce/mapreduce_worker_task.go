package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"math"
	"os"
	"strconv"

	pb "./proto"
	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/pkg/etcdutil"
	"github.com/taskgraph/taskgraph"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const nonExistWork = math.MaxUint64

type workerTask struct {
	framework taskgraph.Framework
	taskType  string
	epoch     uint64
	logger    *log.Logger
	taskID    uint64
	workID    uint64

	//channels
	epochChange  chan *mapreduceEvent
	dataReady    chan *mapreduceEvent
	metaReady    chan *mapreduceEvent
	finishedChan chan *mapreduceEvent
	notifyChan   chan *mapreduceEvent
	exitChan     chan struct{}
	stopGrabTask chan bool

	//io writer
	shuffleDepositWriter bufio.Writer

	mapreduceConfig MapreduceConfig
}

type shuffleEmit struct {
	Key   string   `json:"key"`
	Value []string `json:"value"`
}

type mapperEmitKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type mapreduceEvent struct {
	ctx      context.Context
	epoch    uint64
	fromID   uint64
	workID   uint64
	linkType string
	meta     string
}

func (mp *mapreduceTask) Clean(path string) {
	err := mp.mapreduceConfig.FilesystemClient.Remove(path)
	if err != nil {
		mp.logger.Fatal(err)
	}
}

func (mp *mapreduceTask) Collect(key string, val string) {
	mp.outputWriter.Write([]byte(key + " " + val + "\n"))
}

func (mp *mapreduceTask) Init(taskID uint64, framework taskgraph.Framework) {
	mp.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	mp.taskID = taskID
	mp.framework = framework
	mp.finishedTask = make(map[uint64]bool)
	//channel init
	mp.stopGrabTask = make(chan bool, 1)
	mp.epochChange = make(chan *mapreduceEvent, 1)
	mp.dataReady = make(chan *mapreduceEvent, 1)
	mp.metaReady = make(chan *mapreduceEvent, 1)
	mp.exitChan = make(chan struct{})
	go mp.run()
}

func (mp *mapreduceTask) run() {
	for {
		select {
		case ec := <-mp.epochChange:
			go mp.doEnterEpoch(ec.ctx, ec.epoch)

		case notify := <-mp.notifyChan:
			mp.framework.FlagMeta(notify.ctx, notify.linkType, notify.meta)

		case <-mp.exitChan:
			return

		case dataReady := <-dataReady:
			go mp.processWork(dataReady.ctx, dataReady.fromID, dataReady.method, dataReady.output)
		}
	}
}

// By type of task, decide eclusive progress to run
func (mp *mapreduceTask) initializeTaskEnv(ctx context.Context) {
}

func (mp *mapreduceTask) processWork(ctx context.Context, workChan chan *mapreduceEvent) {

}

// Read given file, process it through mapper function by user setting
func (mp *mapreduceTask) fileRead(ctx context.Context, work taskgraph.Work) {
	file := work.Config["inputFile"]
	mp.logger.Printf("FileReader, Mapper task %d, process %s, %s", mp.taskID, file, mp.mapreduceConfig.InterDir)
	var i uint64
	mp.mapperWriteCloser = make([]bufio.Writer, 0)
	for i = 0; i < mp.mapreduceConfig.ReducerNum; i++ {
		path := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(i, 10) + "from" + strconv.FormatUint(mp.workID, 10)
		mp.logger.Println("Output Path ", path)
		mp.Clean(path)
		tmpWrite, err := mp.mapreduceConfig.FilesystemClient.OpenWriteCloser(path)
		if err != nil {
			mp.logger.Fatalf("MapReduce : get azure storage client writer failed, ", err)
		}
		mp.mapperWriteCloser = append(mp.mapperWriteCloser, *bufio.NewWriterSize(tmpWrite, mp.mapreduceConfig.WriterBufferSize))
	}

	mapperReaderCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(file)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client reader failed, ", err)
	}
	// if mp.workID == 3 {
	// 	mp.logger.Fatalf("failed by intention")
	// }
	err = nil
	var str string
	bufioReader := bufio.NewReaderSize(mapperReaderCloser, mp.mapreduceConfig.ReaderBufferSize)
	for err != io.EOF {
		str, err = bufioReader.ReadString('\n')

		if err != io.EOF && err != nil {
			mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		mp.mapreduceConfig.MapperFunc(mp, str)
	}
	mapperReaderCloser.Close()
	for i = 0; i < mp.mapreduceConfig.ReducerNum; i++ {
		mp.mapperWriteCloser[i].Flush()
	}
	mp.logger.Println("FileRead finished")
	// notify the master mapper work has been done
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, workID: mp.workID, fromID: mp.taskID, linkType: "Slave", meta: "MapperWorkFinished" + strconv.FormatUint(mp.workID, 10)}
	// release the task to grab a new work
	mp.etcdClient.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, strconv.FormatUint(mp.taskID, 10)), false)
}

// Read mapper data, shuffle, set a new reducer work
func (mp *mapreduceTask) transferShuffleData(ctx context.Context) {
	mp.logger.Println("In transfer Data function")
	mp.shuffleContainer = make(map[string][]string)
	workNum := len(mp.mapreduceConfig.WorkDir["mapper"])
	for i := 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.Itoa(i)
		shuffleReadCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(shufflePath)
		mp.logger.Println("get shuffle data from ", shufflePath)
		if err != nil {
			mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		}
		bufioReader := bufio.NewReaderSize(shuffleReadCloser, mp.mapreduceConfig.ReaderBufferSize)
		var str []byte
		err = nil
		for err != io.EOF {
			str, err = bufioReader.ReadBytes('\n')
			if err != io.EOF && err != nil {
				mp.logger.Fatalf("MapReduce : Shuffle read Error, ", err)
			}
			if err != io.EOF {
				str = str[:len(str)-1]
			}
			mp.processShuffleKV(str)
		}
	}

	tranferPath := mp.mapreduceConfig.InterDir + "/shuffle" + strconv.FormatUint(mp.workID, 10)
	mp.Clean(tranferPath)
	mp.logger.Println("output shuffle data to ", tranferPath)
	shuffleWriteCloser, err := mp.mapreduceConfig.FilesystemClient.OpenWriteCloser(tranferPath)
	bufferWriter := bufio.NewWriterSize(shuffleWriteCloser, mp.mapreduceConfig.WriterBufferSize)
	if err != nil {
		mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
	}
	for k := range mp.shuffleContainer {
		block := &shuffleEmit{
			Key:   k,
			Value: mp.shuffleContainer[k],
		}
		data, err := json.Marshal(block)
		if err != nil {
			mp.logger.Fatalf("Shuffle Emit json error, %v\n", err)
		}
		data = append(data, '\n')
		bufferWriter.Write(data)
	}
	bufferWriter.Flush()
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType: "Slave", meta: "ShuffleWorkFinished"}
	key := etcdutil.FreeWorkPathForType(mp.mapreduceConfig.AppName, "reducer", strconv.FormatUint(mp.workID, 10))
	mp.logger.Println("shuffle finished, add reducer work ", key)

	mp.etcdClient.Set(key, "begin", 0)
	for i := 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.Itoa(i)
		mp.Clean(shufflePath)
	}
	mp.etcdClient.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, strconv.FormatUint(mp.taskID, 10)), false)

}

func (mp *mapreduceTask) getNodeTaskType() string {
	master := len(mp.framework.GetTopology().GetNeighbors("Master", mp.epoch))
	if master == 0 {
		return "master"
	}
	switch mp.epoch {
	case 0:
		if mp.taskID < mp.mapreduceConfig.MapperNum {
			return "mapper"
		}
		return "shuffle"
	case 1:
		if mp.taskID < mp.mapreduceConfig.ShuffleNum {
			return "shuffle"
		}
		return "reducer"
	}
	return "reducer"
}

func (mp *mapreduceTask) reducerProcess(ctx context.Context) {
	outputPath := mp.mapreduceConfig.OutputDir + "/reducer" + strconv.FormatUint(mp.workID, 10)
	mp.Clean(outputPath)
	outputWrite, err := mp.mapreduceConfig.FilesystemClient.OpenWriteCloser(outputPath)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	mp.outputWriter = bufio.NewWriterSize(outputWrite, mp.mapreduceConfig.WriterBufferSize)
	reducerPath := mp.mapreduceConfig.InterDir + "/shuffle" + strconv.FormatUint(mp.workID, 10)

	reducerReadCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(reducerPath)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReaderSize(reducerReadCloser, mp.mapreduceConfig.ReaderBufferSize)
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		if err != io.EOF && err != nil {
			mp.logger.Fatalf("MapReduce : Reducer read Error, ", err)
			return
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		mp.processReducerKV(str)
	}
	mp.outputWriter.Flush()
	mp.logger.Printf("%s removing..\n", reducerPath)
	mp.Clean(reducerPath)
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType: "Slave", meta: "ReducerWorkFinished" + strconv.FormatUint(mp.workID, 10)}
	mp.etcdClient.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, strconv.FormatUint(mp.taskID, 10)), false)

}

func (mp *mapreduceTask) processReducerKV(str []byte) {
	var tp shuffleEmit
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		mp.mapreduceConfig.ReducerFunc(mp, tp.Key, tp.Value)
	}
}

func (mp *mapreduceTask) processShuffleKV(str []byte) {
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		mp.shuffleContainer[tp.Key] = append(mp.shuffleContainer[tp.Key], tp.Value)
	}
}

func (mp *workerTask) EnterEpoch(ctx context.Context, epoch uint64) {
	mp.epochChange <- &mapreduceEvent{ctx: ctx, epoch: epoch}
}

func (mp *workerTask) grabWork(ctx context.Context, method string) {
	master := t.framework.GetTopology()["Master"].GetNeighbors(mp.epoch)
	for _, node := range master {
		t.framework.DataRequest(ctx, node, method, &pb.Request{taskID: mp.taskID})
	}
}

func (mp *mapreduceTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	grabWork(ctx, "/proto.Master/GetWork")
}

func (mp *mapreduceTask) Exit() {
	close(mp.stopGrabTask)
	close(mp.exitChan)
}

func (mp *mapreduceTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterMapreduceServer(server, mp)
	return server

}

func (mp *mapreduceTask) CreateOutputMessage(method string) proto.Message { return nil }

func (mp *mapreduceTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.dataReady <- &event{ctx: ctx, fromID: fromID, method: method, output: output}
}

func (m *mapreduceTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	m.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}
