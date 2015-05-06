package mapreduce

// user_interface provide user a interface to start their mapreduce application
// MapreduceBootstrapController would controll the bootstrap of controller, task
// (serves as same as regression/demo/run_regression.sh).
// It likes a local version of Kubernetes, managing the whole framework status.
// For user starting a new mapreduce framework, only need to do is
// invoke NewMapreduceBootstrapController() to get a new controlle,
// invoke its Start interface with his own mapreduce configuration(represents as map[string]interface{})
// therefore the mapreduce framework will run automatically.

import "log"

type MapreduceController interface {
	Start(map[string]interface{})
}

type MapreduceBootstrapController struct {
	Config map[string]interface{}
	logger *log.Logger
}

func NewMapreduceBootstrapController() MapreduceBootstrapController {
	return MapreduceBootstrapController{}
}
