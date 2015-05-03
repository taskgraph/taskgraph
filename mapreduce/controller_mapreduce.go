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
	Config taskgraph.MapreduceConfig
	logger *log.Logger
}

func (mpc *mapreduceController) setFreeWork() {
	etcdClient := etcd.NewClient(mpc.Config.EtcdURLs)
	for i := 0; i < len(mpc.Config.WorkDir["mapper"]); i++ {
		etcdutil.MustCreate(etcdClient, mpc.logger, etcdutil.FreeWorkPathForType(mpc.Config.AppName, "mapper", strconv.Itoa(i)), "", 0)

	}
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (mpc *mapreduceController) runController(ntask uint64) {
	controller := controller.New(mpc.Config.AppName, etcd.NewClient(mpc.Config.EtcdURLs), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
	controller.Start()
	controller.WaitForJobDone()
}

func (mpc *mapreduceController) runBootstrap() {
	bootstrap := framework.NewMapreduceBootStrap(mpc.Config.AppName, mpc.Config.EtcdURLs, createListener(), mpc.logger)
	taskBuilder := &MapreduceTaskBuilder{}
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(NewMapReduceTopology(mpc.Config.MapperNum, mpc.Config.ShuffleNum, mpc.Config.ReducerNum))
	bootstrap.InitWithMapreduceConfig(mpc.Config)
	bootstrap.Start()
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
	mpc.Config = config

	ntask := max(config.MapperNum+config.ShuffleNum, config.ShuffleNum+config.ReducerNum) + 1
	go mpc.runController(ntask)
	mpc.setFreeWork()
	var i uint64 = 0
	mpc.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	for ; i < ntask+5; i++ {
		go mpc.runBootstrap()
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
