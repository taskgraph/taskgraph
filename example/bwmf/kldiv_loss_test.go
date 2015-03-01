package bwmf

import (
	"fmt"
	"math"
	"testing"

	"github.com/taskgraph/taskgraph/op"
)

func TestEvaluation(t *testing.T) {
	h := taskgraph_op.NewVecParameter(2)
	w := taskgraph_op.NewVecParameter(3)
	g := taskgraph_op.NewVecParameter(2)
	v := make([]map[int]float32, 3)
	wh := make([][]float32, 3)
	for i, _ := range v {
		v[i] = make(map[int]float32)
		wh[i] = make([]float32, 2)
	}

	// matrix v
	v[0][0] = 0.5
	v[0][1] = 0.5
	v[1][0] = 1.0
	v[2][1] = 1.0

	// fixed factor
	w.Set(0, 0.33)
	w.Set(1, 0.33)
	w.Set(2, 0.33)

	// given parameter
	h.Set(0, 0.5)
	h.Set(1, 0.5)

	kld_loss := KLDivLoss{
		V:  v,
		WH: wh,
		W:  w,
		m:  3,
		n:  2,
		k:  1,
	}

	loss_val := kld_loss.Evaluate(h, g)

	fmt.Println(kld_loss.V)

	fmt.Println("loss val is ", loss_val)
	fmt.Println("gradient is ", g)

	if math.Abs(float64(loss_val)-2.68) > 1e-1 {
		t.Errorf("Loss value incorrect: actual %f, expected 0.5", loss_val)
	}

	if math.Abs(float64(g.Get(0))+2.0) > 1e-1 || math.Abs(float64(g.Get(1))+2.0) > 1e-1 {
		t.Error("Gradient incorrect: actual {%f, %f}, expected {2.0, 2.0}", g.Get(0), g.Get(1))
	}
}
