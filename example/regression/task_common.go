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
}

func (tk *taskCommon) Init(framework taskgraph.Framework, numberOfTasks uint64) {
	tk.taskID = framework.TaskID()
	tk.framework = framework
	tk.treeTopo = topo.NewTreeTopology(2, numberOfTasks)
	tk.treeTopo.SetTaskID(numberOfTasks)
	tk.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
