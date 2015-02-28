package regression

import "github.com/taskgraph/taskgraph"

type parameterProcessor struct {
	parameter *data
}

func (proc *parameterProcessor) Compute(ins []taskgraph.InboundChannel, outs []taskgraph.OutboundChannel) {
	outs[0].Send(proc.parameter.serialize())
}

type gradientProcessor struct {
	parameter *data
}

func (proc *gradientProcessor) Compute(ins []taskgraph.InboundChannel, outs []taskgraph.OutboundChannel) {
	if proc.parameter == nil {
		// this is a slave
		proc.parameter = deserialzeData(ins[0].Data())
	}
	proc.createLocalGradient(proc.parameter)
	for _, in := range ins {
		childG := deserialzeData(in.Data())
		proc.updateLocalGradient(childG)
	}
	outs[0].Send(proc.localGradient().serialize())
}

func (proc *gradientProcessor) createLocalGradient(parameter *data) {}
func (proc *gradientProcessor) updateLocalGradient(childG *data)    {}
func (proc *gradientProcessor) localGradient() *data {
	panic("")
}
