package mapreduce

// user_interface provide user a interface to start their mapreduce application
// MapreduceBootstrapController would controll the bootstrap of controller, task
// (serves as same as regression/demo/run_regression.sh).
// It likes a local version of Kubernetes, managing the whole framework status.
// For user starting a new mapreduce framework, only need to do is
// invoke NewMapreduceBootstrapController() to get a new controlle,
// invoke its Start interface with his own mapreduce configuration(represents as map[string]interface{})
// therefore the mapreduce framework will run automatically.

import "fmt"

type MRBootstrap struct {
	Config map[string]interface{}
}

func NewMRBootstrap(userConfig map[string]interface{}) NewMRBootstrap {
	return &NewMRBootstrap{
		Config: userConfig,
	}
}

const defaultBufferSize = 4096

func (mpc *MRBootstrap) SetConfig() error {
	mpc.Config = config
	err := mpc.checkConfiguration()
	if err != nil {
		return err
	}
	return nil
}

// check whether the key exists in config or not,
// true represents it does not exist, false opposites
func (mpc *MRBootstrap) checkConfigurationExist(key string) bool {
	_, exist := mpc.Config[key]
	return !exist
}

func (mpc *MRBootstrap) checkConfiguration() {
	// check the least mapreduce framework configuration
	if mpc.checkConfigurationExist("AppName") {
		return fmt.Errorf("Miss the configuration of Application Name")
	}
	if mpc.checkConfigurationExist("MapperNum") || mpc.checkConfigurationExist("ShuffleNum") || mpc.checkConfigurationExist("ReducerNum") {
		return fmt.Errorf("Miss the configuration of Mapper/Shuffle/Reducer Num")
	}
	if mpc.checkConfigurationExist("FilesystemClient") {
		return fmt.Errorf("Miss the configuration of Filesystem Client")
	}
	if mpc.checkConfigurationExist("mapperFunc") || mpc.checkConfigurationExist("reducerFunc") {
		return fmt.Errorf("Miss the configuration of Mapper/Reducer process function")
	}
	if mpc.checkConfigurationExist("outputDir") {
		return fmt.Errorf("Miss the output path")
	}

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
}
