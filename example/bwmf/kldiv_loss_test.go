package bwmf

import (
	"fmt"
	"math"
	"testing"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/op"
)

func TestEvaluation(t *testing.T) {
	h := op.NewVecParameter(2)
	w := op.NewVecParameter(3)
	g := op.NewVecParameter(2)
	v := &pb.SparseMatrixShard{Row: make([]*pb.SparseMatrixShard_SparseRow, 3)}
	wh := make([][]float32, 3)

	for i, _ := range v.Row {
		v.Row[i] = &pb.SparseMatrixShard_SparseRow{At: make(map[int32]float32)}
		wh[i] = make([]float32, 2)
	}

	// matrix v
	v.Row[0].At[0] = 0.5
	v.Row[0].At[1] = 0.5
	v.Row[1].At[0] = 1.0
	v.Row[2].At[1] = 1.0

	// fixed factor
	w.Set(0, 0.33)
	w.Set(1, 0.33)
	w.Set(2, 0.33)

	// given parameter
	h.Set(0, 0.5)
	h.Set(1, 0.5)

	kld_loss := KLDivLoss{
		V:      v,
		WH:     wh,
		W:      w,
		m:      3,
		n:      2,
		k:      1,
		smooth: 1e-6,
	}

	loss_val := kld_loss.Evaluate(h, g)

	fmt.Println(kld_loss.V)

	fmt.Println("loss val is ", loss_val)
	fmt.Println("gradient is ", g)

	if math.Abs(float64(loss_val)-4.6) > 5e-2 {
		t.Errorf("Loss value incorrect: actual %f, expected 6.06543", loss_val)
	}

	if math.Abs(float64(g.Get(0))+1.01) > 1e-2 || math.Abs(float64(g.Get(1))+1.01) > 1e-2 {
		t.Errorf("Gradient incorrect: actual {%f, %f}, expected { -1.01, -1.01 }", g.Get(0), g.Get(1))
	}
}
