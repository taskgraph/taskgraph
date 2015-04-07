package op

import (
	"fmt"
	"testing"
)

func calcRMS(a, b Parameter) float64 {
	sum := float64(0)
	for it := a.IndexIterator(); it.Next(); {
		i := it.Index()
		diff := a.Get(i) - b.Get(i)
		sum += float64(diff * diff)
	}
	return sum / float64(a.IndexIterator().Size())
}

// We need to test projected_gradient so that we have the confidence to use it
// in other places.
func TestPGMinimize0(t *testing.T) {
	copy := 1
	epsilon := 1e-5
	lower := NewAllTheSameParameter(-20.0, copy*2)
	upper := NewAllTheSameParameter(+20.0, copy*2)
	ground_truth := NewAllTheSameParameter(1.0, copy*2)

	proj := &Projection{lower_bound: lower, upper_bound: upper}
	minimizer := &ProjectedGradient{projector: proj, beta: 0.1, sigma: 0.01, alpha: 1.0}
	loss := &Rosenbrock{numOfCopies: copy, count: 0}
	stpCount := MakeFixCountStopCriteria(1200)

	stt0 := createParam(3.0, -2.0, copy)

	minimizer.Minimize(loss, stpCount, stt0)
	fmt.Println(stt0)

	rms0 := calcRMS(stt0, ground_truth)
	if rms0 > epsilon {
		t.Errorf("RMS error larger than threshold: actual %f, expected %f", rms0, epsilon)
	}
}

func TestPGMinimize1(t *testing.T) {
	copy := 20
	epsilon := 1e-5
	lower := NewAllTheSameParameter(-20.0, copy*2)
	upper := NewAllTheSameParameter(+20.0, copy*2)
	ground_truth := NewAllTheSameParameter(1.0, copy*2)

	proj := &Projection{lower_bound: lower, upper_bound: upper}
	minimizer := &ProjectedGradient{projector: proj, beta: 0.1, sigma: 0.01, alpha: 1.0}
	loss := &Rosenbrock{numOfCopies: copy, count: 0}
	stpNorm := MakeGradientNormStopCriteria(1.0e-4)
	stt1 := createParam(15.0, -10.0, copy)

	minimizer.Minimize(loss, stpNorm, stt1)
	fmt.Println(stt1)

	rms1 := calcRMS(stt1, ground_truth)
	if rms1 > epsilon {
		t.Errorf("RMS error larger than threshold: actual %f, expected %f", rms1, epsilon)
	}
}

func TestPGMinimize2(t *testing.T) {
	copy := 100
	epsilon := 1e-4
	lower := NewAllTheSameParameter(-20.0, copy*2)
	upper := NewAllTheSameParameter(+20.0, copy*2)
	ground_truth := NewAllTheSameParameter(1.0, copy*2)

	proj := &Projection{lower_bound: lower, upper_bound: upper}
	loss := &Rosenbrock{numOfCopies: copy, count: 0}
	minimizer := &ProjectedGradient{projector: proj, beta: 0.1, sigma: 0.01, alpha: 1.0}
	stpComposed := MakeComposedCriterion(MakeFixCountStopCriteria(1e6), MakeGradientNormStopCriteria(1.0e-3))
	stt2 := createParam(15.0, -10.0, copy)

	minimizer.Minimize(loss, stpComposed, stt2)
	fmt.Println(stt2)

	rms2 := calcRMS(stt2, ground_truth)
	if rms2 > epsilon {
		t.Errorf("RMS error larger than threshold: actual %f, expected %f", rms2, epsilon)
	}
}
