package regression

import "github.com/taskgraph/taskgraph"

type slaveTask struct {
	taskCommon
}

func (tk *slaveTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	if epoch == tk.totalIteration {
		return
	}
	tk.setupParameterJoint(ctx)
	tk.setupGradientJoint(ctx)
}

func (tk *slaveTask) setupParameterJoint(ctx taskgraph.Context) {
	cp := ctx.CreateComposer()
	cp.SetJoint(&parameterJoint{})

	for _, parent := range tk.treeTopo.GetParents() {
		cp.CreateInboundChannel(parent, "parameter")
	}

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateOutboundChannel(child, "parameter")
	}

	cp.Compose()
}

func (tk *slaveTask) setupGradientJoint(ctx taskgraph.Context) {
	cp := ctx.CreateComposer()
	cp.SetJoint(&gradientJoint{})

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateInboundChannel(child, "gradient")
	}

	for _, parent := range tk.treeTopo.GetParents() {
		cp.CreateInboundChannel(parent, "parameter")
		cp.CreateOutboundChannel(parent, "gradient")
	}

	cp.Compose()
}
