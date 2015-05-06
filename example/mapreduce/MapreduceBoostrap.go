package mapreduce

// user_interface provide user a interface to start their mapreduce application
// MapreduceBootstrapController would controll the bootstrap of controller, task
// (serves as same as regression/demo/run_regression.sh).
// It likes a local version of Kubernetes, managing the whole framework status.
// For user starting a new mapreduce framework, only need to do is
// invoke NewMapreduceBootstrapController() to get a new controlle,
// invoke its Start interface with his own mapreduce configuration(represents as map[string]interface{})
// therefore the mapreduce framework will run automatically.

import (
	"log"
	"net"
	"os"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/controller"
)

type MapreduceBootstrap interface {
	Start(map[string]interface{})
}

type MapreduceBootstrapController struct {
	Config map[string]interface{}
	logger *log.Logger
}

func NewMapreduceBootstrapController() MapreduceBootstrapController {
	return MapreduceBootstrapController{}
}

const defaultBufferSize = 4096
var controllerStarted chan bool

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}


// runController allocate a new controller to start initial configuration
func (mpc *MapreduceBootstrapController) runController(ntask uint64) {
	controller := controller.New(convert(mpc.Config["AppName"].(string)), etcd.NewClient(mpc.Config.EtcdURLs.([]string)), uint64(ntask), []string{"Prefix", "Suffix", "Master", "Slave"})
	controller.Start()
	controllerStarted <- true
	controller.WaitForJobDone()
}

// bootstrap controller start a new task node
// transmit the configuration to task builder
func (mpc *MapreduceBootstrapController) runBootstrap() {
	bootstrap := framework.NewBootStrap(mpc.Config["AppName"].(string), mpc.Config.EtcdURLs.([]string), createListener(), ll)
	taskBuilder := &MapreduceTaskBuilder{Config: mpc.Config}
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(NewMapReduceTopology(mpc.Config["MapperNum"].(uint64), mpc.Config["ShuffleNum"].(uint64), mpc.Config.["ReducerNum"].(uint64)))
	bootstrap.Start()
}

// check whether the key exists in config or not,
// true represents it does not exist, false opposites
func (mpc *MapreduceBootstrapController) checkConfigurationExist(key string) bool {
	_, exist := config[key]; 
	return !exist
}

func (mpc *MapreduceBootstrapController) Start(config taskgraph.MapreduceConfig) error {
	
	mpc.Config = config
	// check the least mapreduce framework configuration
	if mpc.checkConfigurationExist("AppName") {
		return fmt.Errorf("Miss the configuration of Application Name")
	}
	if mpc.checkConfigurationExist("MapperNum") || mpc.checkConfigurationExist("ShuffleNum") || mpc.checkConfigurationExist("ReducerNum")) {
		return fmt.Errorf("Miss the configuration of Mapper/Shuffle/Reducer Num")
	}
	if mpc.checkConfigurationExist("FilesystemClient") {
		return fmt.Errorf("Miss the configuration of Filesystem Client")
	}
	if mpc.checkConfigurationExist("mapperFunc") || mpc.checkConfigurationExist("reducerFunc") {
		return fmt.Errorf("Miss the configuration of Mapper/Reducer process function")
	}
	if mpc.checkConfigurationExist("outputDir") || mpc

	// set default configuration, if user doesn't define
	if mpc.checkConfigurationExist("EtcdURLs") { 
		mpc.config["EtcdURLs"] = []string{"http://localhost:4001"}
	}
	if mpc.checkConfigurationExist("InterDir") {
		mpc.config["InterDir"] = "MapreducerProcessTemporaryResult"
	}
	if mpc.checkConfigurationExist("ReaderBufferSize") {
		mpc.config["ReaderBufferSize"] = defaultBufferSize
	}
	if mpc.checkConfigurationExist("WriterBufferSize") {
		mpc.config["WriterBufferSize"] = defaultBufferSize
	}
	if mpc.checkConfigurationExist("FreeNode") {
		mpc.config["FreeNode"] = 3
	}
	
	// calculate the maximum number of node coexist during all epochs
	// plus one represents that thers is a reservation serving for master node
	ntask := max(config["MapperNum"]+config["ShuffleNum"], config["ShuffleNum"]+config["ReducerNum"]) + 1
	controllerStarted = make(chan bool, 1)

	// Issue : could controller create a free work?
	// I've not fingure out current framework how to create free work
	// at my previous code, I directly add some path (Like FreeworkDir) the layout  
	// and add some value to this path
	go mpc.runController(ntask)

	// wait controller initialization finished
	<- controllerStarted

	// mpc.setFreeWork()

	mpc.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	for i := (uint64)(0); i < ntask + mpc.config["FreeNode"].(uint64); i++ {
		go mpc.runBootstrap()
	}

	return nil
	
}

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
