package regression

import "github.com/taskgraph/taskgraph"

type slaveTask struct {
	taskCommon
}

func (tk *slaveTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	if epoch == tk.totalIteration {
		return
	}
	tk.setupParameterProcessor(ctx)
	tk.setupGradientProcessor(ctx)
}

func (tk *slaveTask) setupParameterProcessor(ctx taskgraph.Context) {
	cp := ctx.CreateComposer()
	cp.SetProcessor(&parameterProcessor{})

	for _, parent := range tk.treeTopo.GetParents() {
		cp.CreateInboundChannel(parent, "parameter")
	}

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateOutboundChannel(child, "parameter")
	}

	cp.Compose()
}

func (tk *slaveTask) setupGradientProcessor(ctx taskgraph.Context) {
	cp := ctx.CreateComposer()
	cp.SetProcessor(&gradientProcessor{})

	for _, child := range tk.treeTopo.GetChildren() {
		cp.CreateInboundChannel(child, "gradient")
	}

	for _, parent := range tk.treeTopo.GetParents() {
		cp.CreateInboundChannel(parent, "parameter")
		cp.CreateOutboundChannel(parent, "gradient")
	}

	cp.Compose()
}
