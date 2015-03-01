package taskgraph_op

import (
	"math"
	"testing"
)

import "fmt"

// We need to test projected_gradient so that we have the confidence to use it
// in other places.
func TestPGMinimize0(t *testing.T) {
	copy := 1
	lower := NewAllTheSameParameter(-40000.0, copy*2)
	upper := NewAllTheSameParameter(+40000.0, copy*2)

	proj := &Projection{lower_bound: lower, upper_bound: upper}

	minimizer := &ProjectedGradient{projector: proj, beta: 0.1, sigma: 0.01, alpha: 1.0}

	loss := &Rosenbrock{numOfCopies: copy, count: 1}

	stt := createParam(-1.2, 2.4, copy)

	stp := MakeGradientNormStopCriteria(1.0e-6)

	minimizer.Minimize(loss, stp, stt)

	fmt.Print("(%g, %g)", stt.Get(0), stt.Get(1))

	epsilon := 0.0000001
	if math.Abs(float64(1.0-stt.Get(0))) < epsilon {
		t.Fail()
	}
}
