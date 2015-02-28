package regression

import (
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/factory"
)

type slaveTask struct {
	taskCommon
}

func (tk *slaveTask) Run(framework taskgraph.Framework, numberOfTasks uint64) {
	tk.Init(framework, numberOfTasks)
	go func() {
		for {
			tk.eventHandler()
		}
	}()
}

func (tk *slaveTask) eventHandler() {
	select {
	case epoch := <-tk.epochChan:
		tk.SetEpoch(epoch)
	case <-tk.exitChan:
	}
}

func (tk *slaveTask) SetEpoch(epoch uint64) {
	if epoch == tk.totalIteration {
		return
	}
	tk.setupGradientProcessor()
}

func (tk *slaveTask) setupParameterProcessor() {
	cp := factory.CreateComposer()
	cp.SetProcessor(&parameterProcessor{})

	for _, from := range tk.treeTopo.GetParents() {
		cp.CreateInboundChannel(from, "parameter")
	}

	for _, from := range tk.treeTopo.GetChildren() {
		cp.CreateOutboundChannel(from, "parameter")
	}

	cp.Compose()
}

func (tk *slaveTask) setupGradientProcessor() {
	cp := factory.CreateComposer()
	cp.SetProcessor(&gradientProcessor{})

	for _, from := range tk.treeTopo.GetChildren() {
		cp.CreateInboundChannel(from, "gradient")
	}

	for _, from := range tk.treeTopo.GetParents() {
		cp.CreateInboundChannel(from, "parameter")
		cp.CreateOutboundChannel(from, "parameter")
	}

	cp.Compose()
}
