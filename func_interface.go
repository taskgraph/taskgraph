package taskgraph

// We need some interface to define function and how we optimize them.
type Parameter interface {
	GetParam(index uint64) float32
	SetParam(index uint64, value float32)
}

type Gradient interface {
	GetValue() float32
	GetGradient(index uint64) float32
	SetValue(value float32)
	SetGradient(index uint64, grad float32)
}

type Function interface {
	Evaluate(param Parameter) Gradient
}

type Minimizer interface {
	Minimize(func Function, param Parameter) bool
}