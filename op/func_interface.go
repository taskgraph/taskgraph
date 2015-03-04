package taskgraph_op

// For more information read: http://ewencp.org/blog/golang-iterators/
// We need an reasonably efficient way to enumerate all indexes.
//
// For example, we can do:
// sum := 0.0
// for it := param.IndexIterator(); it.Next(); {
//    sum += param.Get(it.Index());
//}
type IndexIterator interface {
	Index() int
	Next() bool
	Rewind()
	Size() int
}

// We need some interface to define function and how we optimize them.
type Parameter interface {
	Get(index int) float32
	Set(index int, value float32)
	Add(index int, value float32)

	// This allow us to generate parameter with same width.
	CloneWithoutCopy() Parameter

	// This allow one to enumerate through all parameters
	IndexIterator() IndexIterator
}

// This func is useful to fill the parameter with the same value
func Fill(param Parameter, value float32) {
	for it := param.IndexIterator(); it.Next(); {
		param.Set(it.Index(), value)
	}
}

// Compute the sum of the element of parameter after some transformation.
func Sum(param Parameter, compute func(float32) float32) float32 {
	sum := float64(0)
	for it := param.IndexIterator(); it.Next(); {
		sum += float64(compute(param.Get(it.Index())))
	}
	return float32(sum)
}

// There are many different ways one can optimize a function, but the
// most effective ones need gradient.
// The interface is designed so that one can compose function easily.
// The semantics is simply add gradient from this function to corresponding
// dimensions on the output paramenter gradient.
type Function interface {
	Evaluate(param Parameter, gradient Parameter) float32
}

// This defines an additive function. One can compose the function this way easily.
type SumFunction struct {
	func1, func2 Function
}

func (rf *SumFunction) Evaluate(param Parameter, gradient Parameter) float32 {
	sum := rf.func1.Evaluate(param, gradient)
	sum += rf.func2.Evaluate(param, gradient)
	return sum
}

// This implements l1 l2 regularization.
type Regularization struct {
	iter         IndexIterator
	l1reg, l2reg float32
}

func (r *Regularization) Evaluate(param Parameter, gradient Parameter) float32 {
	r.iter.Rewind()
	sum := float64(0)
	for r.iter.Next() {
		index := r.iter.Index()
		value := param.Get(index)
		sum += float64(r.l1reg * value)
		sum += float64(0.5 * r.l2reg * value * value)
		gradient.Add(index, r.l1reg)
		gradient.Add(index, r.l2reg*value)
	}
	return float32(sum)
}

// This is used to figure out where one can stop the optimization. Implementation
// can be count based, or gradient norm based.
type StopCriteria interface {
	Done(param Parameter, value float32, gradient Parameter) bool
}

// This stop criteria stop after some fix iterations.
type fixCountStopCriteria struct {
	maxIter, curIter int
}

func (f *fixCountStopCriteria) Done(param Parameter, value float32, gradient Parameter) bool {
	f.curIter += 1
	return f.curIter >= f.maxIter
}

func MakeFixCountStopCriteria(iter int) StopCriteria {
	return &fixCountStopCriteria{maxIter: iter, curIter: 0}
}

// This criteria stop the iteration when the norm of gradient below some predefined threshold
type gradNormStopCriteria struct {
	grad_norm_thres float32
}

func (g *gradNormStopCriteria) Done(param Parameter, value float32, gradient Parameter) bool {
	norm := Sum(param, func(x float32) float32 { return x * x })
	return norm < g.grad_norm_thres
}

func MakeGradientNormStopCriteria(thres float32) StopCriteria {
	return &gradNormStopCriteria{grad_norm_thres: thres}
}

// High level interface for minimization. This assume that we start
// with one point in the parameter space, and end with an optimal
// point. Return true if we find optimal point.
type Minimizer interface {
	Minimize(function Function, stopCriteria StopCriteria, param Parameter) bool
}
