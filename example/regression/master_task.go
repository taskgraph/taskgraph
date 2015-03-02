package regression

import (
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/job"
)

type masterTask struct {
	taskCommon
	parameter *data
}

func (tk *masterTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	if epoch == tk.totalIteration {
		tk.framework.ShutdownJob(job.SuccessStatus)
		return
	}
	tk.setupParameterProcessor(ctx)
	tk.setupGradientProcessor(ctx)
}

func (tk *masterTask) setupParameterProcessor(ctx taskgraph.Context) {
	// It's a source point because it doesn't have any inbound chan.
	cp := ctx.CreateComposer()
	cp.SetProcessor(&parameterProcessor{
		parameter: tk.parameter,
	})

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateOutboundChannel(child, "parameter")
	}

	cp.Compose()
}

func (tk *masterTask) setupGradientProcessor(ctx taskgraph.Context) {
	// This is a sync point because it doesn't have any outbound chan.
	cp := ctx.CreateComposer()
	cp.SetProcessor(&gradientProcessor{
		parameter: tk.parameter,
	})

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateInboundChannel(child, "gradient")
	}

	cp.Compose()
}
