package regression

import "github.com/taskgraph/taskgraph"

type parameterJoint struct {
	parameter *data
}

func (proc *parameterJoint) Compute(ins []taskgraph.InboundChannel, outs []taskgraph.OutboundChannel) {
	if proc.parameter == nil {
		proc.parameter = unmarshalData(ins[0].Get())
	}
	for _, child := range outs {
		child.Put(proc.parameter)
	}
}

type gradientJoint struct {
	parameter *data
}

func (proc *gradientJoint) Compute(ins []taskgraph.InboundChannel, outs []taskgraph.OutboundChannel) {
	// master task have parameter already. slave task doesn't have, so he needs to
	// retrieve from others.
	if proc.parameter == nil {
		proc.parameter = unmarshalData(ins[0].Get())
	}
	proc.createLocalGradient(proc.parameter)
	for _, in := range ins {
		childG := unmarshalData(in.Get())
		proc.updateLocalGradient(childG)
	}
	outs[0].Put(proc.localGradient())
}

func (proc *gradientJoint) createLocalGradient(parameter *data) {}
func (proc *gradientJoint) updateLocalGradient(childG *data)    {}
func (proc *gradientJoint) localGradient() *data {
	panic("")
}
