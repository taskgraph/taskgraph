package regression

import "github.com/taskgraph/taskgraph/factory"

type slaveTask struct {
	taskCommon
	totalIteration uint64
}

func (tk *slaveTask) SetEpoch(epoch uint64) {
	if epoch == tk.totalIteration {
		return
	}
	tk.setupGradientProcessor()
}

func (tk *slaveTask) setupGradientProcessor() {
	cp := factory.CreateComposer()
	cp.SetProcessor(&gradientProcessor{})

	for _, from := range tk.treeTopo.GetChildren() {
		inChan := CreateInChannel(from, "gradient")
		cp.AttachInboundChannel(inChan)
	}

	for _, from := range tk.treeTopo.GetParents() {
		inChan := CreateInChannel(from, "parameter")
		cp.AttachInboundChannel(inChan)

		outChan := CreateOutChannel(from, "gradient")
		cp.AttachOutboundChannel(outChan)
	}

	cp.Compose()
}
