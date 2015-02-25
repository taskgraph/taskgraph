package bwmf

import (
	"math"
)

var Inf = math.Inf(1)

type MatrixOps interface {
}

type MatrixMetrics interface {
	Dims() (int, int)
	Norm(f int) float64
	Sum() float64 // sum of the elements
	Det() float64
}

type MatrixData interface {
	At(r, c int) float64
	// Row(r int)
}

type MatrixMutator interface {
	Set(r, c int, value float64)
	NewMatrix(r, c int)
}

// The abstraction of the matrix interface is quite specific to the algorithm described in [Chih-Jen Lin 2007] ``Projected Gradient Methods for Non-negative Matrix Factorization''

//
type DenseRow []float64
type SparseRow map[int]float64 // we assume the operation on a row is not concurrent

////////////////////////////////
var (
	denseMatrix *DenseMatrix
	_           MatrixOps     = denseMatrix
	_           MatrixMetrics = denseMatrix
	_           MatrixData    = denseMatrix
	_           MatrixMutator = denseMatrix
)

type DenseMatrix struct {
	row, column int
	data        []DenseRow
}

func (m *DenseMatrix) Dims() (int, int) {
	return m.row, m.column
}

func (m *DenseMatrix) Norm(f int) float64 {
	if f == 0 {
		// TODO
		return 0.0
	} else if f == 1 {
		// TODO
		return 0.0
	} else if f == 2 {
		// TODO
		return 0.0
	} else if f == -1 {
		// -1 for inf norm
		// TODO
		return 0.0
	} else {
		panic("")
	}
}

func (m *DenseMatrix) Sum() float64 {
	// TODO
	return 0.0
}

func (m *DenseMatrix) Det() float64 {
	// TODO
	return 0.0
}

func (m *DenseMatrix) At(r, c int) float64 {
	// TODO
	return 0.0
}

func (m *DenseMatrix) Row(r int) DenseRow {
	return m.data[r]
}

func (m *DenseMatrix) Set(r, c int, value float64) {
	m.data[r][c] = value
}

func (m *DenseMatrix) NewMatrix(r, c int) {
	m.row = r
	m.column = c
	m.data = make([]DenseRow, r)
	// foreach row
	for i, _ := range m.data {
		m.data[i] = make([]float64, c)
	}
}

////////////////////////////////
var (
	sparseMatrix *SparseMatrix
	_            MatrixOps     = sparseMatrix
	_            MatrixMetrics = sparseMatrix
	_            MatrixData    = sparseMatrix
	_            MatrixMutator = sparseMatrix
)

type SparseMatrix struct {
	row, column int
	data        []SparseRow
}

func (m *SparseMatrix) Dims() (int, int) {
	return m.row, m.column
}

func (m *SparseMatrix) Norm(f int) float64 {
	if f == 0 {
		// TODO
		return 0.0
	} else if f == 1 {
		// TODO
		return 0.0
	} else if f == 2 {
		// TODO
		return 0.0
	} else if f == -1 {
		// -1 for inf norm
		// TODO
		return 0.0
	} else {
		panic("")
	}
}

func (m *SparseMatrix) Sum() float64 {
	// TODO
	return 0.0
}

func (m *SparseMatrix) Det() float64 {
	// TODO
	return 0.0
}

func (m *SparseMatrix) At(r, c int) float64 {
	// TODO
	value, ok := m.data[r][c]
	if ok {
		return value
	} else {
		return 0.0
	}
}

func (m *SparseMatrix) Row(r int) SparseRow {
	return m.data[r]
}

func (m *SparseMatrix) Set(r, c int, value float64) {
	m.data[r][c] = value
}

func (m *SparseMatrix) NewMatrix(r, c int) {
	m.row = r
	m.column = c
	m.data = make([]SparseRow, r)
	// foreach row
	for i, _ := range m.data {
		m.data[i] = make(map[int]float64)
	}
}

// blas
