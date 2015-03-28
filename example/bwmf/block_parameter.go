package bwmf

import (
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	op "github.com/taskgraph/taskgraph/op"
)

// This is an encapsulation for exposing an op.Parameter interface with a pb.DenseMatrix as the underlying data storage.
type blockParameter struct {
	k    int
	data *pb.DenseMatrixShard
}

func NewBlockParameter(matrix *pb.DenseMatrixShard) op.Parameter {
	return &blockParameter{k: len(matrix.Row[0].At), data: matrix}
}

func (bp *blockParameter) Get(index int) float32 {
	r := index / bp.k
	c := index % bp.k
	return bp.data.Row[r].At[c]
}

// we do not Clone to a blockParamter, instead we return a vecParameter.
func (bp *blockParameter) CloneWithoutCopy() op.Parameter {
	return op.NewVecParameter(bp.k * len(bp.data.Row))
}

func (bp *blockParameter) IndexIterator() op.IndexIterator {
	return &blockParameterIterator{row: 0, column: -1, size: bp.k * len(bp.data.Row), k: bp.k, m: len(bp.data.Row)}
}

// `Set` and `Add` are made panic to make blockParamter immutable.
func (bp *blockParameter) Set(index int, value float32) {
	panic("")
}
func (bp *blockParameter) Add(index int, value float32) {
	panic("")
}

type blockParameterIterator struct {
	row, column, size int
	k, m              int // num of columns and rows
}

func (it *blockParameterIterator) Index() int {
	return it.row*it.k + it.column
}

func (it *blockParameterIterator) Next() bool {
	it.column += 1
	if it.column >= it.k {
		it.row += 1
	}
	return it.row >= it.m
}

func (it *blockParameterIterator) Rewind() {
	it.row = 0
	it.column = -1
}

func (it *blockParameterIterator) Size() int {
	return it.size
}
