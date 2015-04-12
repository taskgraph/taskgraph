package bwmf

import (
	"fmt"
	"testing"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/op"
)

func reportError(value, actual, expected int, where string, t *testing.T) {
	t.Errorf("%s testing failed. value: %d, actual: %d, expected %d.", where, value, actual, expected)
}

func TestBSearch(t *testing.T) {
	a := []int{0, 100, 101, 200, 300, 400, 500, 520}
	v := []int{0, 10, 100, 102, 101, 200, 299, 300, 399, 400, 499, 500}
	r := []int{0, 0, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6}

	fmt.Println("Testing binary search of blockId...")
	for i, _ := range v {
		ra, err := bsearch(a, v[i])
		if ra != r[i] || err != nil {
			reportError(v[i], ra, r[i], "BSearch", t)
		}
	}
	ra, err := bsearch(a, 520)
	if err == nil {
		reportError(520, ra, -1, "BSearch", t)
	}

	b := a[0:2]
	ra2, err2 := bsearch(b, 10)
	if ra2 != 0 || err2 != nil {
		reportError(10, ra2, 0, "BSearch", t)
	}
	fmt.Println("BSearching of blockId Done.")
}

func TestBlocksParameter(t *testing.T) {
	matrices, param := paramFromNewBlocksParameter()

	fmt.Println("matrices: ", *matrices)
	fmt.Println("param: ", param)

	checkParam(param, t)
	param.Set(4, 1000.0)
	param.Add(7, 2000.0)
	a1 := (*matrices)[0].Row[2].At[0]
	if a1 != 1000.0 {
		t.Errorf("Testing setting failed. Expected 1000.0, actual %f.", a1)
	}
	a2 := (*matrices)[1].Row[0].At[1]
	if a2 != 2008.0 {
		t.Errorf("Testing setting failed. Expected 2008.0, actual %f.", a2)
	}

	param.Set(4, 3.0)
	param.Set(7, 8.0)
	checkIter(param, t)
}

func TestSingleBlockParameter(t *testing.T) {
	matrix, param := paramFromNewSingleBlockParameter()
	fmt.Println("matrix: ", *matrix)
	fmt.Println("param: ", param)

	checkParam(param, t)
	param.Set(4, 1000.0)
	param.Add(7, 2000.0)
	a1 := matrix.Row[2].At[0]
	if a1 != 1000.0 {
		t.Errorf("Testing setting failed. Expected 1000.0, actual %f.", a1)
	}
	a2 := matrix.Row[3].At[1]
	if a2 != 2008.0 {
		t.Errorf("Testing setting failed. Expected 2008.0, actual %f.", a2)
	}

	param.Set(4, 3.0)
	param.Set(7, 8.0)
	checkIter(param, t)
}

func paramFromNewBlocksParameter() (*map[uint64]*pb.DenseMatrixShard, op.Parameter) {
	// 2x2 and 3x3 matrices
	matrices := make(map[uint64]*pb.DenseMatrixShard)
	matrices[0] = &pb.DenseMatrixShard{
		Row: []*pb.DenseMatrixShard_DenseRow{
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
		},
	}
	matrices[1] = &pb.DenseMatrixShard{
		Row: []*pb.DenseMatrixShard_DenseRow{
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
		},
	}
	for i, _ := range matrices[0].Row {
		for j, _ := range matrices[0].Row[i].At {
			matrices[0].Row[i].At[j] = float32((i + 1) * (j + 1))
		}
	}
	for i, _ := range matrices[1].Row {
		for j, _ := range matrices[1].Row[i].At {
			matrices[1].Row[i].At[j] = float32((i + 4) * (j + 1))
		}
	}
	param := NewBlocksParameter(&matrices)
	return &matrices, param
}

func paramFromNewSingleBlockParameter() (*pb.DenseMatrixShard, op.Parameter) {
	// one 5x2 matrix
	matrix := &pb.DenseMatrixShard{
		Row: []*pb.DenseMatrixShard_DenseRow{
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
			&pb.DenseMatrixShard_DenseRow{At: make([]float32, 2)},
		},
	}
	for i, _ := range matrix.Row {
		for j, _ := range matrix.Row[i].At {
			matrix.Row[i].At[j] = float32((i + 1) * (j + 1))
		}
	}
	param := NewSingleBlockParameter(matrix)
	return matrix, param
}

func checkParam(param op.Parameter, t *testing.T) {
	for i := 0; i < 5; i++ {
		for j := 0; j < 2; j++ {
			act := param.Get(2*i + j)
			xpt := float32((i + 1) * (j + 1))
			if act != xpt {
				t.Errorf("Param Getting failed. Expected %f, actual %f.", xpt, act)
			}
		}
	}
}

func checkIter(param op.Parameter, t *testing.T) {
	iter := param.IndexIterator()
	for i := 0; i < 5; i++ {
		for j := 0; j < 2; j++ {
			if !iter.Next() {
				t.Errorf("Iterator breaks before the end.")
			}
			act := param.Get(iter.Index())
			xpt := float32((i + 1) * (j + 1))
			if act != xpt {
				t.Errorf("Iterating to (%d,%d) failed. Expected %f, actual %f.", i, j, xpt, act)
			}
		}
	}

	if iter.Next() {
		t.Errorf("Iterator fails to end at the end.")
	}

	iter.Rewind()
	for i := 0; i < 5; i++ {
		for j := 0; j < 2; j++ {
			if !iter.Next() {
				t.Errorf("Iterator breaks before the end.")
			}
			act := param.Get(iter.Index())
			xpt := float32((i + 1) * (j + 1))
			if act != xpt {
				t.Errorf("Iterating to (%d,%d) failed. Expected %f, actual %f.", i, j, xpt, act)
			}
		}
	}
}
