package mapreduce

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"io"
	"log"
	"math"
	"os"
	"path"
	"strconv"

	"../../taskgraph"
	"./pkg/etcdutil"
	pb "./proto"
	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const nonExistWork = math.MaxUint64

type mapreduceTask struct {
	framework         taskgraph.Framework
	taskType          string
	epoch             uint64
	logger            *log.Logger
	taskID            uint64
	workID            uint64
	mapperNumCount    uint64
	shuffleNumCount   uint64
	reducerNumCount   uint64
	outputWriter      *bufio.Writer
	etcdClient        *etcd.Client
	mapperWriteCloser []bufio.Writer

	finishedTask     map[uint64]bool
	config           map[string]string
	shuffleContainer map[string][]string
	lenFinishedTask  uint64

	//channels
	epochChange  chan *mapreduceEvent
	dataReady    chan *mapreduceEvent
	metaReady    chan *mapreduceEvent
	finishedChan chan *mapreduceEvent
	notifyChan   chan *mapreduceEvent
	stopGrabTask chan *mapreduceEvent
	exitChan     chan struct{}

	//work channels
	mapperWorkChan  chan *mapreduceEvent
	shuffleWorkChan chan *mapreduceEvent
	reducerWorkChan chan *mapreduceEvent

	//io writer
	shuffleDepositWriter bufio.Writer

	mapreduceConfig taskgraph.MapreduceConfig
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

func (mp *mapreduceTask) Emit(key, val string) {
	if mp.mapreduceConfig.ShuffleNum == 0 {
		return
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	var KV mapperEmitKV
	KV.Key = key
	KV.Value = val
	toShuffle := h.Sum32() % uint32(mp.mapreduceConfig.ReducerNum)
	data, err := json.Marshal(KV)
	data = append(data, '\n')
	if err != nil {
		mp.logger.Fatalf("json marshal error : ", err)
	}
	mp.mapperWriteCloser[toShuffle].Write(data)
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
	mp.epochChange = make(chan *mapreduceEvent, 1)
	mp.dataReady = make(chan *mapreduceEvent, 1)
	mp.metaReady = make(chan *mapreduceEvent, 1)
	mp.exitChan = make(chan struct{})
	mp.notifyChan = make(chan *mapreduceEvent, 1)
	mp.mapperWorkChan = make(chan *mapreduceEvent, 1)
	mp.shuffleWorkChan = make(chan *mapreduceEvent, 1)
	mp.reducerWorkChan = make(chan *mapreduceEvent, 1)
	go mp.run()
}

func (mp *mapreduceTask) MapreduceConfiguration(mapreduceConfig taskgraph.MapreduceConfig) {
	mp.mapreduceConfig = mapreduceConfig
	mp.etcdClient = etcd.NewClient(mapreduceConfig.EtcdURLs)
}

func (mp *mapreduceTask) run() {
	for {
		select {
		case ec := <-mp.epochChange:
			mp.doEnterEpoch(ec.ctx, ec.epoch)

		// case done := <-mp.finishedChan:
		// 	mp.framework.FlagMeta(done.ctx, done.linkType, done.message)

		case notify := <-mp.notifyChan:
			mp.framework.FlagMeta(notify.ctx, notify.linkType, notify.meta)

		case metaReady := <-mp.metaReady:
			go mp.processMessage(metaReady.ctx, metaReady.fromID, metaReady.linkType, metaReady.meta)

		case <-mp.exitChan:
			return
		case work := <-mp.mapperWorkChan:
			go mp.fileRead(work.ctx, mp.mapreduceConfig.WorkDir["mapper"][mp.workID])
		case work := <-mp.shuffleWorkChan:
			go mp.transferShuffleData(work.ctx)
		case work := <-mp.reducerWorkChan:
			go mp.reducerProcess(work.ctx)
		}
	}
}

func (mp *mapreduceTask) processMessage(ctx context.Context, fromID uint64, linkType string, meta string) {
	switch mp.taskType {
	// case 2 :
	// 	switch meta {
	// 		case "mapperDataNeedCommit":
	// 			go shuffleCommitMapperData(fromID)
	// 	}
	// case 1 :
	// 	switch meta {
	// 		case "shuffleProcessFinished":
	// 				go processMapperWork(ctx)
	// 	}
	case "master":
		switch meta {
		case "MapperWorkFinished":
			mp.mapperNumCount++
			if mp.mapreduceConfig.MapperNum <= mp.mapperNumCount {
				mp.framework.IncEpoch(ctx)
			}
		case "ReducerWorkFinished":
			mp.reducerNumCount++
			if mp.mapreduceConfig.ReducerNum <= mp.reducerNumCount {
				mp.framework.ShutdownJob()
			}
		}

	}
}

func (mp *mapreduceTask) initializeTaskEnv(ctx context.Context) {
	close(mp.stopGrabTask)
	mp.stopGrabTask = make(chan *mapreduceEvent, 1)
	switch mp.taskType {
	case "mapper":
		go mp.processWork(ctx, mp.mapperWorkChan)
	case "shuffle":
		if mp.epoch == 1 {
			go mp.processWork(ctx, mp.shuffleWorkChan)
		}
	case "reducer":
		go mp.processWork(ctx, mp.reducerWorkChan)
	case "master":
		mp.mapperNumCount = 0
		mp.reducerNumCount = 0
	}
}

func (mp *mapreduceTask) grabWork(grabWorkType string) error {
	for {
		freeWork, err := etcdutil.WaitFreeNode(etcdutil.FreeWorkDirForType(mp.mapreduceConfig.AppName, grabWorkType), mp.etcdClient, mp.logger)
		if err != nil {
			return err
		}
		mp.logger.Printf("standby grabbed free work %d", freeWork)
		idStr := strconv.FormatUint(freeWork, 10)
		ok, err := etcdutil.TryOccupyNode(
			etcdutil.FreeWorkPathForType(mp.mapreduceConfig.AppName, grabWorkType, idStr),
			"",
			etcdutil.TaskMasterWorkForType(mp.mapreduceConfig.AppName, grabWorkType, idStr),
			mp.etcdClient,
			path.Join(mp.taskType, strconv.FormatUint(freeWork, 10)),
		)
		if err != nil {
			return err
		}
		if ok {
			mp.workID = freeWork
			return nil
		}
		select {
		case <-mp.stopGrabTask:
			return nil
		}
		mp.logger.Printf("standby tried work %d failed. Wait free work again.", freeWork)
	}
}

func (mp *mapreduceTask) processWork(ctx context.Context, workChan chan *mapreduceEvent) {
	receiver := make(chan *etcd.Response, 1)
	stop := make(chan bool, 1)
	go mp.etcdClient.Watch(etcdutil.FreeWorkDirForType(mp.mapreduceConfig.AppName, mp.taskType), 0, false, receiver, stop)
	var num uint64 = 0
	for resp := range receiver {
		num = 0
		mp.logger.Printf("Mapreduce : %s task process No.%d work", mp.taskType, num)
		if resp.Action != "delete" {
			continue
		}
		mp.workID = nonExistWork
		err := mp.grabWork(mp.taskType)
		if err != nil {
			mp.logger.Fatalf("MapReduce : Mapper task grab work Error, ", err)
		}
		if mp.workID != nonExistWork {
			workChan <- &mapreduceEvent{ctx: ctx}
		} else {
			return
		}
	}
}

// Read given file, process it through mapper function by user setting
func (mp *mapreduceTask) fileRead(ctx context.Context, work taskgraph.Work) {
	file := work.Config["inputFile"]
	mp.logger.Printf("FileReader, Mapper task %d, process s", mp.taskID, file)
	var i uint64
	mp.mapperWriteCloser = make([]bufio.Writer, 0)
	for i = 0; i < mp.mapreduceConfig.ReducerNum; i++ {
		path := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(i, 10) + "from" + strconv.FormatUint(mp.workID, 10)
		mp.Clean(path)
		tmpWrite, err := mp.mapreduceConfig.FilesystemClient.OpenWriteCloser(file)
		if err != nil {
			mp.logger.Println("MapReduce : get azure storage client writer failed, ", err)
		}
		mp.mapperWriteCloser = append(mp.mapperWriteCloser, *bufio.NewWriterSize(tmpWrite, mp.mapreduceConfig.WriterBufferSize))
	}

	mapperReaderCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(file)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client reader failed, ", err)
	}
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
	mp.logger.Println("FileRead finished")
	mp.etcdClient.Delete(etcdutil.TaskMasterWorkForType(mp.mapreduceConfig.AppName, mp.taskType, strconv.FormatUint(mp.taskID, 10)), false)
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, workID: mp.workID, fromID: mp.taskID, linkType: "Slave", meta: "MapperWorkFinished"}
	// mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType : "Prefix", meta : "mapperDataNeedCommit"}
}

// func (mp *mapreduceTask) shuffleCommitMapperData(uint64 fromID) {
// 	conveyReader, err := mp.mapreduceConfig.filesystemClient.OpenReadClose(mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(fromID, 10))
// 	if err != nil {
// 		mp.logger.Fatalf("MapReduce : shuffle commit error, ", err)
// 	}
// 	bufioReader := bufio.NewReaderSize(shuffleReadCloser, mp.mapreduceConfig.ReadBufferSize)
// 	mp.etcdClient.Get(key, sort, recursive)
// 	work, err := client.Get(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, fromID), false, false)
// 	if err != nil {
// 		return err
// 	}
// 	commitPath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.taskID - mp.mapreduceConfig.MapperNum, 10)  + "from" + strconv.FormatUint(work.Value, 10)
// 	mp.Clean(commitPath)
// 	conveyWriter, err := mp.mapreduceConfig.filesystemClient.OpenWriteClose(commitPath)
// 	if err != nil {
// 		mp.logger.Fatalf("MapReduce : shuffle commit error, ", err)
// 	}
// 	bufioWriter := bufio.NewReaderSize(shuffleReadCloser, mp.mapreduceConfig.ReadBufferSize)
// 	n, err := bufioReader.WriterTo(bufioWriter)
// 	if err != nil {
// 		mp.logger.Fatalf("MapReduce : shuffle commit error, ", err)
// 	}
// 	mp.finishedTask[work.Value] = true
// 	client.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, fromID), false)
// 	if len(mp.finishedTask) == mp.lenFinishedTask {
// 		mp.notifyChan <- &mapperEvent{ctx: ctx, epoch: mp.epoch, linkType : "Slave", meta : "shuffleCommitFinished"}
// 	}
// }

func (mp *mapreduceTask) transferShuffleData(ctx context.Context) {
	mp.shuffleContainer = make(map[string][]string)
	workNum := len(mp.mapreduceConfig.WorkDir["mapper"])
	for i := 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.Itoa(i)
		shuffleReadCloser, err := mp.mapreduceConfig.FilesystemClient.OpenReadCloser(shufflePath)
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
	_ = etcdutil.MustCreate(mp.etcdClient, mp.logger, key, "", 0)
	for i := 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.Itoa(i)
		mp.Clean(shufflePath)
	}

}

func (mp *mapreduceTask) getNodeTaskType() string {
	prefix := len(mp.framework.GetTopology().GetNeighbors("Prefix", mp.epoch))
	suffix := len(mp.framework.GetTopology().GetNeighbors("Suffix", mp.epoch))
	master := len(mp.framework.GetTopology().GetNeighbors("Master", mp.epoch))
	if master == 0 {
		return "master"
	}
	if prefix == 0 {
		if mp.epoch == 0 {
			return "mapper"
		}
		return "shuffle"
	}
	if suffix == 0 {
		if mp.epoch == 0 {
			return "shuffle"
		}
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
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType: "Slave", meta: "ReducerWorkFinished"}
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

// func (mp *mapreduceTask) shuffleProcess() {
// 	mp.shuffleDepositWriter.Write(p)

// 	mp.finished <- &shuffleEvent{ctx: ctx}

// }

// At present, epoch is not a required parameter for mapper
// but it may be useful in the future
func (mp *mapreduceTask) EnterEpoch(ctx context.Context, epoch uint64) {
	mp.epochChange <- &mapreduceEvent{ctx: ctx, epoch: epoch}
}

func (mp *mapreduceTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	mp.logger.Printf("doEnterEpoch, Mapper task %d, epoch %d", mp.taskID, epoch)
	mp.epoch = epoch
	mp.taskType = mp.getNodeTaskType()
	mp.initializeTaskEnv(ctx)
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
}

func (m *mapreduceTask) MetaReady(ctx context.Context, fromID uint64, LinkType, meta string) {
	m.metaReady <- &mapreduceEvent{ctx: ctx, fromID: fromID, linkType: LinkType, meta: meta}
}