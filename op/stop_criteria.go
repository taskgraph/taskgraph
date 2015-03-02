package taskgraph_op

import (
	"time"
)

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
	start time.Time
	limit time.Duration
}

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
	for _, ic := range c.criterion {
		if ic.Done(param, value, gradient) {
			return true
		}
	}
	return false
}
