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
	tk.setupParameterJoint(ctx)
	tk.setupGradientJoint(ctx)
}

func (tk *masterTask) setupParameterJoint(ctx taskgraph.Context) {
	// It's a source point because it doesn't have any inbound chan.
	cp := ctx.CreateComposer()
	cp.SetJoint(&parameterJoint{
		parameter: tk.parameter,
	})

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateOutboundChannel(child, "parameter")
	}

	cp.Compose()
}

func (tk *masterTask) setupGradientJoint(ctx taskgraph.Context) {
	// This is a sync point because it doesn't have any outbound chan.
	cp := ctx.CreateComposer()
	cp.SetJoint(&gradientJoint{
		parameter: tk.parameter,
	})

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateInboundChannel(child, "gradient")
	}

	cp.Compose()
}
