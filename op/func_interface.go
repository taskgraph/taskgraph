package taskgraph

// For more information read: http://ewencp.org/blog/golang-iterators/
// We need an reasonably efficient way to enumerate all indexes.
//
// For example, we can do:
// sum := 0.0
// for it := param.GetIndexIterator(); it.Next(); {
//    sum += param.Get(it.Index());
//}
type IndexIterator interface {
	Index() uint64
	Next() bool
}

// We need some interface to define function and how we optimize them.
type Parameter interface {
	Get(index uint64) float32
	Set(index uint64, value float32)

	// This allow us to generate a same wide parameter for manipulation.
	Clone() Parameter

	// This allow one to enumerate through all parameters
	IndexIterator() IndexIterator
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