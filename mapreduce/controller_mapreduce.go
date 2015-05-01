package mapreduce

import (
	"log"
	"net"
	"os"
	"strconv"

	"../../taskgraph"
	"../framework"
	"../mapreduce/pkg/etcdutil"
	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/controller"
)

const defaultBufferSize = 4096

type MapreduceController interface {
	Start(taskgraph.MapreduceConfig)
}

type mapreduceController struct {
	config taskgraph.MapreduceConfig
	logger *log.Logger
}

func (mpc *mapreduceController) setFreeWork() {
	etcdClient := etcd.NewClient(mpc.config.EtcdURLs)
	for i := 0; i < len(mpc.config.WorkDir["mapper"]); i++ {
		etcdutil.MustCreate(etcdClient, mpc.logger, etcdutil.FreeWorkPathForType(mpc.config.AppName, "mapper", strconv.Itoa(i)), "", 0)

	}
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (mpc *mapreduceController) Start(config taskgraph.MapreduceConfig) {

	if len(config.EtcdURLs) == 0 {
		config.EtcdURLs = []string{"http://localhost:4001"}
	}
	if config.InterDir == "" {
		config.InterDir = "MapreducerProcessTemporaryResult"
	}
	if config.ReaderBufferSize == 0 {
		config.ReaderBufferSize = defaultBufferSize
	}
	if config.WriterBufferSize == 0 {
		config.WriterBufferSize = defaultBufferSize
	}
	mpc.config = config
	mpc.setFreeWork()

	ntask := max(config.MapperNum+config.ShuffleNum, config.ShuffleNum+config.ReducerNum) + 1
	controller := controller.New(config.AppName, etcd.NewClient(config.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
	controller.Start()
	controller.WaitForJobDone()
	var i uint64 = 0
	mpc.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	for ; i < ntask+5; i++ {
		bootstrap := framework.NewMapreduceBootStrap(config.AppName, config.EtcdURLs, createListener(), mpc.logger)
		taskBuilder := &MapreduceTaskBuilder{}
		bootstrap.SetTaskBuilder(taskBuilder)
		bootstrap.SetTopology(NewMapReduceTopology(config.MapperNum, config.ShuffleNum, config.ReducerNum))
		bootstrap.InitWithMapreduceConfig(config)
		bootstrap.Start()
	}
	// New Controller

	// New a set of bootstrap

	// run free node
}

func NewMapreduceController() mapreduceController {
	return mapreduceController{}
}

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
