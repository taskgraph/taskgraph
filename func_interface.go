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

// There are many different ways one can optimize a function, but the
// most effective ones need gradient.
type Function interface {
	Evaluate(param Parameter) Gradient
}

// High level interface for minimization. This assume that we start
// with one point in the parameter space, and end with an optimal
// point. Return true if we find optimal point.
type Minimizer interface {
	Minimize(func Function, param Parameter) bool
}