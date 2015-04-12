package bwmf

import (
	"fmt"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/op"
)

// This is an encapsulation for exposing an op.Parameter interface with a pb.DenseMatrix as the underlying data storage.
type blockParameter struct {
	k      int
	data   *map[uint64]*pb.DenseMatrixShard
	starts []int // for binary search of blockId
	size   int
}

func NewBlocksParameter(matrices *map[uint64]*pb.DenseMatrixShard) op.Parameter {
	stt := make([]int, len(*matrices)+1)
	ost := 0
	for i := 0; i < len(*matrices); i++ {
		stt[i] = ost
		ost += len((*matrices)[uint64(i)].Row)
	}
	stt[len(*matrices)] = ost
	k := len((*matrices)[0].Row[0].At)
	return &blockParameter{k: k, data: matrices, starts: stt, size: k * ost}
}

func NewSingleBlockParameter(matrix *pb.DenseMatrixShard) op.Parameter {
	k := len(matrix.Row[0].At)
	l := len(matrix.Row)
	return &blockParameter{k: k, data: &map[uint64]*pb.DenseMatrixShard{0: matrix}, starts: []int{0, l}, size: k * l}
}

func bsearch(s []int, v int) (int, error) {
	if v >= s[len(s)-1] {
		return -1, fmt.Errorf("Index Exceeded upper bound.")
	}
	l := 0
	r := len(s)
	m := 0
	for l < r {
		m = (l + r) / 2

		if s[m] > v {
			r = m - 1
		} else if s[m] <= v && s[m+1] > v {
			return m, nil
		} else {
			l = m + 1
		}
	}
	return r, nil
}

func (bp *blockParameter) indexToBlockRC(index int) (b, r, c int, ok bool) {
	ir := index / bp.k
	b, e := bsearch(bp.starts, ir)
	if e != nil {
		// exceeded.
		return -1, -1, -1, false
	}
	r = ir - bp.starts[b]
	c = index % bp.k
	return b, r, c, true
}

func (bp *blockParameter) Get(index int) float32 {
	b, r, c, ok := bp.indexToBlockRC(index)
	if !ok {
		return 0.0
	}
	return (*bp.data)[uint64(b)].Row[r].At[c]
}

// we do not Clone to a blockParamter, instead we return a vecParameter.
func (bp *blockParameter) CloneWithoutCopy() op.Parameter {
	return op.NewVecParameter(bp.size)
}

func (bp *blockParameter) IndexIterator() op.IndexIterator {
	return &blockParameterIterator{
		block:  0,
		row:    0,
		column: -1,
		size:   bp.size,
		bp:     bp,
	}
}

// `Set` and `Add` are made panic to make blockParamter immutable.
func (bp *blockParameter) Set(index int, value float32) {
	b, r, c, ok := bp.indexToBlockRC(index)
	if !ok {
		return
	}
	(*bp.data)[uint64(b)].Row[r].At[c] = value
}

func (bp *blockParameter) Add(index int, value float32) {
	b, r, c, ok := bp.indexToBlockRC(index)
	if !ok {
		return
	}
	(*bp.data)[uint64(b)].Row[r].At[c] += value
}

type blockParameterIterator struct {
	block, row, column int
	size               int
	bp                 *blockParameter
}

func (it *blockParameterIterator) Index() int {
	return (it.bp.starts[it.block]+it.row)*it.bp.k + it.column
}

func (it *blockParameterIterator) Next() bool {
	it.column += 1
	if it.column >= it.bp.k {
		it.row += 1
		if it.row >= it.bp.starts[it.block+1]-it.bp.starts[it.block] {
			it.block += 1
			it.row = 0
		}
		it.column = 0
	}
	return it.block < len(it.bp.starts)-1 // exceeded when equals
}

func (it *blockParameterIterator) Rewind() {
	it.block = 0
	it.row = 0
	it.column = -1
}

func (it *blockParameterIterator) Size() int {
	return it.size
}
