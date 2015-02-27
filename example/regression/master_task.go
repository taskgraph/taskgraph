package regression

import (
	"github.com/taskgraph/taskgraph/factory"
	"github.com/taskgraph/taskgraph/job"
)

type masterTask struct {
	taskCommon
	totalIteration uint64
}

func (tk *masterTask) SetEpoch(epoch uint64) {
	if epoch == tk.totalIteration {
		tk.framework.ShutdownJob(job.SuccessStatus)
		return
	}
	tk.setupParameterProcessor()
	tk.setupGradientProcessor()
}

func (tk *masterTask) setupParameterProcessor() {
	cp := factory.CreateComposer()
	cp.SetProcessor(&parameterProcessor{})

	for _, to := range tk.treeTopo.GetChildren() {
		outChan := CreateOutChannel(to, "parameter")
		cp.AttachOutboundChannel(outChan)
	}

	cp.Compose()
}

func (tk *masterTask) setupGradientProcessor() {
	// This is a sync point because it doesn't have any outbound chan.
	cp := factory.CreateComposer()
	cp.SetProcessor(&gradientProcessor{})

	for _, from := range tk.treeTopo.GetChildren() {
		inChan := CreateInChannel(from, "gradient")
		cp.AttachInboundChannel(inChan)
	}

	cp.Compose()
}
