package bwmf

import (
	"fmt"
	// "math"
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

	fmt.Println("KLDIVLOSS is: ", kld_loss)

	loss_val := kld_loss.Evaluate(h, g)

	fmt.Println("loss val is ", loss_val)
	fmt.Println("gradient is ", g)

	/*
		if math.Abs(float64(loss_val)-4.6) > 5e-2 {
			t.Errorf("Loss value incorrect: actual %f, expected 6.06543", loss_val)
		}

		if math.Abs(float64(g.Get(0))+1.01) > 1e-2 || math.Abs(float64(g.Get(1))+1.01) > 1e-2 {
			t.Errorf("Gradient incorrect: actual {%f, %f}, expected { -1.01, -1.01 }", g.Get(0), g.Get(1))
		}
	*/

	h.Set(0, 0.0)
	h.Set(1, 1.0)
	h.Set(2, 1.0)
	h.Set(3, 0.0)

	loss_val = kld_loss.Evaluate(h, g)

	fmt.Println("loss val is ", loss_val)
	fmt.Println("gradient is ", g)

}

func newKLDivLoss() *KLDivLoss {
	m := 3
	n := 2
	k := 2

	// v is
	// -------------
	// | 0.0 | 1.0 |
	// |-----|-----|
	// | 1.0 | 0.0 |
	// |-----|-----|
	// | 0.5 | 0.5 |
	// |-----|-----|
	v := &pb.MatrixShard{
		Row: []*pb.MatrixShard_RowData{
			&pb.MatrixShard_RowData{RowId: 0, At: map[int32]float32{1: 1.0, 2: 0.5}},
			&pb.MatrixShard_RowData{RowId: 1, At: map[int32]float32{0: 1.0, 2: 0.5}},
		},
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
			Row: []*pb.MatrixShard_RowData{
				&pb.MatrixShard_RowData{RowId: 0, At: map[int32]float32{0: 1.0}},
				&pb.MatrixShard_RowData{RowId: 1, At: map[int32]float32{1: 1.0}},
			},
		},
		&pb.MatrixShard{
			Row: []*pb.MatrixShard_RowData{
				&pb.MatrixShard_RowData{RowId: 0, At: map[int32]float32{0: 0.5, 1: 0.5}},
			},
		},
	}

	return NewKLDivLoss(v, w, m, n, k, 1e-6)
}
