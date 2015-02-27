package taskgraph

// For more information read: http://ewencp.org/blog/golang-iterators/
// We need an reasonably efficient way to enumerate all indexes.
type IndexIterator interface {
	Index() uint64
    Next() bool
}

// We need some interface to define function and how we optimize them.
type Parameter interface {
	GetParam(index uint64) float32
	SetParam(index uint64, value float32)
	GetIndexInterator() IndexIterator
}

// There are many different ways one can optimize a function, but the
// most effective ones need gradient.
type Function interface {
	Evaluate(param Parameter) (value float32, gradient Parameter)
}

// High level interface for minimization. This assume that we start
// with one point in the parameter space, and end with an optimal
// point. Return true if we find optimal point.
type Minimizer interface {
	Minimize(func Function, param Parameter) bool
}