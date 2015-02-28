package regression

import (
	"log"
	"os"

	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/example/topo"
)

type taskCommon struct {
	taskID         uint64
	logger         *log.Logger
	framework      taskgraph.Framework
	treeTopo       *topo.TreeTopology
	totalIteration uint64
	exitChan       chan struct{}
	epochChan      chan uint64
}

func (tk *taskCommon) Init(framework taskgraph.Framework, numberOfTasks uint64) {
	tk.taskID = framework.TaskID()
	tk.framework = framework
	tk.treeTopo = topo.NewTreeTopology(2, numberOfTasks)
	tk.treeTopo.SetTaskID(numberOfTasks)
	tk.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func (tk *taskCommon) ExitChan() chan struct{} {
	return tk.exitChan
}

func (tk *taskCommon) EpochChan() chan<- uint64 {
	return tk.epochChan
}
