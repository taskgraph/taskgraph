package taskgraph_op

import (
	"time"
)

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
	start time.Time
	limit time.Duration
}

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
	for _, ic := range c.criterion {
		if ic.Done(param, value, gradient) {
			return true
		}
	}
	return false
}

func MakeComposedCriterion(criteria ...StopCriteria) StopCriteria {
	return &composedCriterion{criterion: criteria}
}
