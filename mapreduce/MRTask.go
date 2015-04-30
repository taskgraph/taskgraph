package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"math"
	"path"
	"strconv"

	"../../taskgraph"
	pb "./proto"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"./pkg/etcdutil"
	"google.golang.org/grpc"
	"github.com/coreos/go-etcd/etcd"
)

const nonExistWork = math.MaxUint64

type mapreduceTask struct {
	framework    taskgraph.Framework
	taskType     string
	epoch        uint64
	logger       *log.Logger
	taskID       uint64
	workID		 uint64 
	mapperFunc   func(taskgraph.MapreduceFramework, string)
	mapperNum    uint64
	shuffleNum   uint64
	reducerNum   uint64
	outputWriter *bufio.Writer
	etcdClient *etcd.Client

	finishedTask    map[uint64]bool
	config          map[string]string
	lenFinishedTask uint64

	//channels
	epochChange  chan *mapreduceEvent
	dataReady    chan *mapreduceEvent
	metaReady    chan *mapreduceEvent
	finishedChan chan *mapreduceEvent
	stopGrabTask chan int
	exitChan     chan struct{}

	//work channels
	mapperWorkChan chan int
	shuffleWorkChan chan int
	reducerWorkChan chan int

	//io writer
	shuffleDepositWriter bufio.Writer

	mapreduceConfig taskgraph.MapreduceConfig
}

type mapreduceEvent struct {
	ctx      context.Context
	epoch    uint64
	fromID   uint64
	linkType string
	meta  string
}

func (mp *mapreduceTask) Emit(key, val string) {
	if mp.mapreduceConfig.shuffleNum == 0 {
		return
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	var KV emitKV
	KV.Key = key
	KV.Value = val
	toShuffle := h.Sum32() % uint32(mp.mapreduceConfig.shuffleNum)
	data, err := json.Marshal(KV)
	data = append(data, '\n')
	if err != nil {
		mp.logger.Fatalf("json marshal error : ", err)
	}
	mp.shuffleWriteCloser[toShuffle].Write(data)
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


func (mp *mapreduceTask) Init(taskID uint64, framework taskgraph.MapreduceFramework) {
	mp.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	mp.taskID = taskID
	mp.framework = framework
	mp.framework.SetMapperOutputWriter()
	mp.mapperFunc = mp.framework.GetMapperFunc()

	mp.finishedTask = make(map[uint64]bool)

	//channel init
	mp.epochChange = make(chan *mapreduceEvent, 1)
	mp.dataReady = make(chan *mapreduceEvent, 1)
	mp.metaReady = make(chan *mapreduceEvent, 1)
	mp.stopGrabTask = make(chan *mapreduceEvent, 1)
	mp.exitChan = make(chan struct{})
	mp.finsihed = make(chan *mapreduceEvent, 1)

	go mp.run()
}

func (mp *mapreduceTask) MapreduceConfiguration(mapreduceConfig MapreduceConfig) {
	mp.mapreduceConfig = mapreduceConfig
	mp.etcdClient = etcd.NewClient(mapreduceConfig.etcdURLs)
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
			go processMessage(metaReady.ctx, metaReady.fromID, metaReady.linkType, metaReady.meta)

		case <-mp.exitChan:
			return
		case work := <-mp.mapperWorkChan:
			go mp.fileRead(ctx, mp.mapreduceConfig.WorkDir["mapper"][workID])
		case work := <-mp.shuffleWorkChan:
			go mp.transferShuffleData(ctx)
		}
	}
}

func (mp *mapreduceTask) processMessage(ctx context.Context, fromID uint64, linkType string, meta string) {
	switch mp.taskType {
	case 2 :
		switch meta {
			case "mapperDataNeedCommit":
				go shuffleCommitMapperData(fromID)
		}
	case 1 :
		switch meta {
			case "shuffleProcessFinished":
 				go processMapperWork(ctx)
		}	
	case 3 :
		case meta:
			case "shuffleCommitFinished":

		
	}
}

// func (mp *mapreduceTask) shuffleInit() {
// 	// write Shuffle Deposit Write in filesytem
// 	// write after commit content
// }


func (mp *mapreduceTask) initializeTaskEnv(ctx context.Context) {
	// go mp.processWork(ctx, )
	switch mp.taskType {
		case "mapper" : go mp.processMapperWork(ctx, mp.mapperWorkChan)
		case "shuffle" : if epoch == 1 { go mp.processShuffleWork(ctx, mp.shuffleWorkChan) }
		case "reducer" : go mp.processReducerWork(ctx, mp.reducerWorkChan)
	}
}

func (mp *mapreduceTask) grabTask(grabWorkType string) error {
	for {
		freeWork, err := etcdutil.WaitFreeNode(etcdutil.FreeWorkDir(mp.mapreduceConfig.AppName, grabWorkType), mp.mapreduceConfig.AppName, f.log)
		if err != nil {
			return err
		}
		mp.logger.Printf("standby grabbed free task %d", freeTask)
		idStr := strconv.FormatUint(freeWork, 10)
		ok, err := etcdutil.TryOccupyNode(
			etcdutil.FreeWorkPathForType(mp.mapreduceConfig.AppName, grabWorkType, idStr),
			"",
			etcdutil.TaskMasterWorkForType(mp.mapreduceConfig.AppName, grabWorkType, idStr),
			mp.etcdClient,
			path.Join,
		)
		if err != nil {
			return err
		}
		if ok {
			mp.workID = freeWork
			return nil
		}
		select {
		case <- mp.stopGrabTask:
			return nil
		}
		f.log.Printf("standby tried work %d failed. Wait free work again.", freeTask)
	}
}

func (mp *mapreduceTask) processWork(ctx context.Context, workChan chan int) {	
	go client.Watch(etcdutil.TaskMasterWork(name), 0, false, receiver, stop)
	var num uint64 = 0
	for resp := range receiver {
		num = 0
		mp.logger.Printf("Mapreduce : %s task process No.%d work", mp.taskType, num)
		if resp.Action != "delete" {
			continue
		}
		mp.workID = nonExistWork
		err := grabTask(mp.taskType)
		if err != nil {
			mp.logger.Fatalf("MapReduce : Mapper task grab work Error, ", err)
		}
		if (mp.workID != nonExistWork) {
			workChan <- 1
		} 
	}
}

// func (mp *mapreduceTask) processMapperWork(ctx context.Context) {	
// 	go client.Watch(HealthyPath(name), 0, false, receiver, stop)
// 	for resp := range receiver {
// 		if resp.Action != "delete" {
// 			continue
// 		}
// 		mp.workID = nonExistWork
// 		err := grabTask("shuffle")
// 		if err != nil {
// 			mp.logger.Fatalf("MapReduce : Mapper task grab work Error, ", err)
// 		}
// 		if (mp.workID != nonExistWork) {
// 			fileRead(ctx, mp.mapreduceConfig.WorkDir["mapper"][workID])
// 			// client.Delete(FreeTaskPath(name, idStr), false)
// 		}
// 		switch 
// 	}
// }

// Read given file, process it through mapper function by user setting
func (mp *mapreduceTask) fileRead(ctx context.Context, work taskgraph.Work) {
	file := work.(taskgraph.MapperWork).InputFile
	mp.logger.Printf("FileReader, Mapper task %d, process s", mp.taskID, file)
	
	
	mapperReaderCloser, err := mp.mapreduceConfig.filesystemClient.OpenReadCloser(file)
	if err != nil {
		mp.logger.Fatalf("MapReduce : get azure storage client reader failed, ", err)
	}
	err = nil
	var str string
	bufioReader := bufio.NewReaderSize(mapperReaderCloser, mp.mapreduceConfig.readerBufferSize)
	for err != io.EOF {
		str, err = bufioReader.ReadString('\n')

		if err != io.EOF && err != nil {
			mp.logger.Fatalf("MapReduce : Mapper read Error, ", err)
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		mp.mapreduceConfig.mapperFunc(mp, str)
	}
	mapperReaderCloser.Close()
	mp.logger.Println("FileRead finished")
			// mp.notifyChan <- &mapreduceEvent{ctx : ctx, fromID : taskID, linkType : "Prefix", meta : "mapperDataNeedCommit"}
	mp.notifyChan <- &mapreduceEvent{ctx: ctx, epoch: mp.epoch, linkType : "Prefix", meta : "mapperDataNeedCommit"}
}

func (mp *mapreduceTask) shuffleCommitMapperData(uint64 fromID) {
	conveyReader, err := mp.mapreduceConfig.filesystemClient.OpenReadClose(mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(fromID, 10))
	if err != nil {
		mp.logger.Fatalf("MapReduce : shuffle commit error, ", err)
	}
	bufioReader := bufio.NewReaderSize(shuffleReadCloser, mp.mapreduceConfig.ReadBufferSize)
	mp.etcdClient.Get(key, sort, recursive)
	work, err := client.Get(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, fromID), false, false)
	if err != nil {
		return err
	}
	conveyWriter, err := mp.mapreduceConfig.filesystemClient.OpenWriteClose(
		mp.mapreduceConfig.InterDir + 
		"/" + 
		strconv.FormatUint(mp.taskID - mp.mapreduceConfig.MapperNum, 10)  + 
		"from" + 
		strconv.FormatUint(work.Value, 10)
	)
	if err != nil {
		mp.logger.Fatalf("MapReduce : shuffle commit error, ", err)
	}
	bufioWriter := bufio.NewReaderSize(shuffleReadCloser, mp.mapreduceConfig.ReadBufferSize)
	n, err := bufioReader.WriterTo(bufioWriter)
	if err != nil {
		mp.logger.Fatalf("MapReduce : shuffle commit error, ", err)
	}
	mp.finishedTask[work.Value] = true
	client.Delete(etcdutil.TaskMasterWork(mp.mapreduceConfig.AppName, fromID), false)
	if len(mp.finishedTask) == mp.lenFinishedTask {
		mp.notifyChan <- &mapperEvent{ctx: ctx, epoch: mp.epoch, linkType : "Slave", meta : "shuffleCommitFinished"}
	}
}

func (mp *mapreduceTask) transferShuffleData() {
	mp.shuffleContainer()
	int workNum = len(mp.mapreduceConfig.Work["mapper"])
	for i = 0; i < workNum; i++ {
		shufflePath := mp.mapreduceConfig.InterDir + "/" + strconv.FormatUint(mp.workID, 10) + "from" + strconv.FormatUint(i, 10) 
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
			mp.processShuffleKV(str)
		}
	}


	tranferPath := mp.mapreduceConfig.InterDir + "/shuffle" + strconv.FormatUint(sf.taskID, 10)
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


	for (int i = 0; i < n; i++) {

	}
}

func (mp *mapreduceTask) getNodeTaskType() int {
	prefix := len(framework.GetTopology().GetNeighbors("Prefix", mp.epoch))
	suffix := len(framework.GetTopology().GetNeighbors("Suffix", mp.epoch))
	master := len(framework.GetTopology().GetNeighbors("Master", mp.epoch))
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

func (rd *reducerTask) reducerProgress(fromID uint64) {
	reducerPath := rd.framework.GetOutputDirName() + "/shuffle" + strconv.FormatUint(fromID, 10)
	client := rd.framework.GetClient()
	reducerReadCloser, err := client.OpenReadCloser(reducerPath)
	if err != nil {
		rd.logger.Fatalf("MapReduce : get azure storage client failed, ", err)
		return
	}
	bufioReader := bufio.NewReaderSize(reducerReadCloser, rd.framework.GetReaderBufferSize())
	var str []byte
	err = nil
	for err != io.EOF {
		str, err = bufioReader.ReadBytes('\n')
		if err != io.EOF && err != nil {
			rd.logger.Fatalf("MapReduce : Reducer read Error, ", err)
			return
		}
		if err != io.EOF {
			str = str[:len(str)-1]
		}
		rd.processKV(str)
	}
	rd.logger.Printf("%s removing..\n", reducerPath)
	rd.framework.Clean(reducerPath)
}

func (mp *mapreduceTask) processReducerKV(str []byte) {
	var tp shuffleEmit
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		rd.reducerFunc(rd.framework, tp.Key, tp.Value)
	}
}

func (mp *mapreduceTask) processShuffleKV(str []byte) {
	var tp mapperEmitKV
	if err := json.Unmarshal([]byte(str), &tp); err == nil {
		mp.shuffleContainer[tp.Key] = append(sf.shuffleContainer[tp.Key], tp.Value)
	}
}


func (mp *mapreduceTask) shuffleProcess() {
	mp.shuffleDepositWriter.Write(p)
	
	sf.finished <- &shuffleEvent{ctx: ctx}

}



// At present, epoch is not a required parameter for mapper
// but it may be useful in the future
func (mp *mapreduceTask) EnterEpoch(ctx context.Context, epoch uint64) {
	mp.epochChange <- &mapperEvent{ctx: ctx, epoch: epoch}
}

func (mp *mapreduceTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	mp.logger.Printf("doEnterEpoch, Mapper task %d, epoch %d", mp.taskID, epoch)
	mp.epoch = epoch
	mp.taskType = mp.getNodeTaskType()
	mp.initializeTaskEnv()
}

func (mp *mapreduceTask) Exit() {
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
