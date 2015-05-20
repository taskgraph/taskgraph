package bwmf

import (
	"fmt"
	"math"
	"testing"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/op"
)

func TestEvaluation(t *testing.T) {

	h := op.NewVecParameter(4)
	g := h.CloneWithoutCopy()
	h.Set(0, 1.0)
	h.Set(1, 1.0)
	h.Set(2, 1.0)
	h.Set(3, 1.0)

	kld_loss := newKLDivLoss()

	loss_val := kld_loss.Evaluate(h, g)

	fmt.Println("loss val is ", loss_val)
	fmt.Println("gradient is ", g)

	expectedLoss := 5.999997
	if math.Abs(float64(loss_val)-expectedLoss) > 1e-5 {
		t.Errorf("Loss value incorrect: actual %f, expected %f", loss_val, expectedLoss)
	}

	expectedGrad := []float32 { 1.2499998, 0.24999973, 0.24999973, 1.2499998 }
	if euclideanDist(g.Data(), expectedGrad) > 1e-5 {
		t.Errorf("Gradient incorrect: actual %v, expected %v", g.Data(), expectedGrad)
	}

	h.Set(0, 0.0)
	h.Set(1, 1.0)
	h.Set(2, 1.0)
	h.Set(3, 0.0)

	loss_val = kld_loss.Evaluate(h, g)

	fmt.Println("loss val is ", loss_val)
	fmt.Println("gradient is ", g)

	expectedLoss = 3.6931434
	if math.Abs(float64(loss_val)-expectedLoss) > 1e-5 {
		t.Errorf("Loss value incorrect: actual %f, expected %f", loss_val, expectedLoss)
	}

	expectedGrad = []float32 { 1, 0, 0, 1 }
	if euclideanDist(g.Data(), expectedGrad) > 1e-5 {
		t.Errorf("Gradient incorrect: actual %v, expected %v", g.Data(), expectedGrad)
	}
}

func newKLDivLoss() *KLDivLoss {
	m := uint32(3)
	n := uint32(2)
	k := uint32(2)

	// v is
	// -------------
	// | 0.0 | 1.0 |
	// |-----|-----|
	// | 1.0 | 0.0 |
	// |-----|-----|
	// | 0.5 | 0.5 |
	// |-----|-----|
	v := &pb.MatrixShard{
		IsSparse: true,
		M: 2,
		N: 3,
		Val: []float32 {1.0, 1.0, 0.5, 0.5},
		Ir: []uint32 {1, 0, 0, 1},
		Jc: []uint32 {0, 1, 2, 4},
	}

	// w is composed by two shards:
	//
	// shard 0:
	//  -------------
	//  | 1.0 | 0.0 |
	//  -------------
	//  | 0.0 | 1.0 |
	//  -------------
	// and shard 1:
	//  -------------
	//  | 0.5 | 0.5 |
	//  -------------
	//
	w := []*pb.MatrixShard{
		&pb.MatrixShard{
			M: 2,
			N: n,
			Val: []float32 {1.0, 0.0, 0.0, 1.0},
		},
		&pb.MatrixShard{
			M: 1,
			N: n,
			Val: []float32 {0.5, 0.5},
		},
	}

	return NewKLDivLoss(v, w, m, n, k, 1e-6)
}

func euclideanDist(x, y []float32) float64 {
	if len(x) != len(y) {
		return math.Inf(1)
	}

	val := 0.0

	for i, xi := range x {
		val += float64((xi-y[i])*(xi-y[i]))
	}
	return val
}
