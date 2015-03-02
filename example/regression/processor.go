package regression

import "github.com/taskgraph/taskgraph"

type parameterProcessor struct {
	parameter *data
}

func (proc *parameterProcessor) Compute(ins []taskgraph.InboundChannel, outs []taskgraph.OutboundChannel) {
	if proc.parameter == nil {
		proc.parameter = deserialzeData(ins[0].Get())
	}
	for _, child := range outs {
		child.Put(proc.parameter)
	}
}

type gradientProcessor struct {
	parameter *data
}

func (proc *gradientProcessor) Compute(ins []taskgraph.InboundChannel, outs []taskgraph.OutboundChannel) {
	// master task have parameter already. slave task doesn't have, so he needs to
	// retrieve from others.
	if proc.parameter == nil {
		proc.parameter = deserialzeData(ins[0].Get())
	}
	proc.createLocalGradient(proc.parameter)
	for _, in := range ins {
		childG := deserialzeData(in.Get())
		proc.updateLocalGradient(childG)
	}
	outs[0].Put(proc.localGradient())
}

func (proc *gradientProcessor) createLocalGradient(parameter *data) {}
func (proc *gradientProcessor) updateLocalGradient(childG *data)    {}
func (proc *gradientProcessor) localGradient() *data {
	panic("")
}
