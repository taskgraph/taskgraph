package regression

import (
	"github.com/taskgraph/taskgraph/factory"
	"github.com/taskgraph/taskgraph/job"
)

type masterTask struct {
	taskCommon
	totalIteration uint64
	parameter      *data
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
	// It's a source point because it doesn't have any inbound chan.
	cp := factory.CreateComposer()
	cp.SetProcessor(&parameterProcessor{
		parameter: tk.parameter,
	})

	for _, to := range tk.treeTopo.GetChildren() {
		cp.CreateOutboundChannel(to, "parameter")
	}

	cp.Compose()
}

func (tk *masterTask) setupGradientProcessor() {
	// This is a sync point because it doesn't have any outbound chan.
	cp := factory.CreateComposer()
	cp.SetProcessor(&gradientProcessor{
		parameter: tk.parameter,
	})

	for _, from := range tk.treeTopo.GetChildren() {
		cp.CreateInboundChannel(from, "gradient")
	}

	cp.Compose()
}
