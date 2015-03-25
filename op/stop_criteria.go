package taskgraph_op

import (
	"time"
)

<<<<<<< HEAD
// This file lists common stopping criteria for optimization.

// Criterion as comparing Frobenius norm with a given tolerance.
type GradNormTolCriterion struct {
	tolerance float32
}

func (c *GradNormTolCriterion) Done(param Parameter, value float32, gradient Parameter) bool {
	var gradNorm float32 = 0.0
	for iter := gradient.IndexIterator(); iter.Next(); {
		v := gradient.Get(iter.Index())
		gradNorm += v * v
	}
	return gradNorm < c.tolerance*c.tolerance
}

// Criterion as checking timeout
type TimeoutCriterion struct {
=======
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
	norm := Sum(gradient, func(x float32) float32 { return x * x })
	return norm < g.grad_norm_thres
}

func MakeGradientNormStopCriteria(thres float32) StopCriteria {
	return &gradNormStopCriteria{grad_norm_thres: thres}
}

// Criterion as checking timeout
type timeoutCriterion struct {
>>>>>>> master
	start time.Time
	limit time.Duration
}

<<<<<<< HEAD
func (c *TimeoutCriterion) Done(param Parameter, value float32, gradient Parameter) bool {
	if time.Now().Sub(c.start) > c.limit {
		return true
	} else {
		return false
	}
}

// Criterion as a combination of criteria
type ComposedCriterion struct {
	criterion []StopCriteria
}

func (c *ComposedCriterion) Done(param Parameter, value float32, gradient Parameter) bool {
=======
func (c *timeoutCriterion) Done(param Parameter, value float32, gradient Parameter) bool {
	return time.Now().Sub(c.start) > c.limit
}

func MakeTimeoutCriterion(limit time.Duration) StopCriteria {
	return &timeoutCriterion{start: time.Now(), limit: limit}
}

// Criterion as a combination of criteria
type composedCriterion struct {
	criterion []StopCriteria
}

func (c *composedCriterion) Done(param Parameter, value float32, gradient Parameter) bool {
>>>>>>> master
	for _, ic := range c.criterion {
		if ic.Done(param, value, gradient) {
			return true
		}
	}
	return false
}
<<<<<<< HEAD
=======

func MakeComposedCriterion(criteria ...StopCriteria) StopCriteria {
	return &composedCriterion{criterion: criteria}
}
>>>>>>> master
