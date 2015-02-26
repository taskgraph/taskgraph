package bwmf

import (
	"math"
	"sync"
)

var Inf = math.Inf(1)

type matrixMetrics interface {
	Dims(trans bool) (int, int)
}

type matrixGetter interface {
	// Get an element
	At(r, c int) float64

	// Get a copy of a row/colomn
	Row(r int) Matrix
	Column(c int) Matrix

	// Get a copy of a sub matrix
	SubMatrix(rs, cs, re, ce int) Matrix
}

type matrixMutator interface {
	Set(r, c int, value float64)
	Init(r, c int) Matrix
}

type Matrix interface {
	matrixMetrics
	matrixGetter
	matrixMutator
}

// The abstraction of the matrix interface is quite specific to the algorithm described in [Chih-Jen Lin 2007] ``Projected Gradient Methods for Non-negative Matrix Factorization''

////////////////////////////////
var (
	denseMatrix *DenseMatrix
	_           Matrix        = denseMatrix
	_           matrixMetrics = denseMatrix
	_           matrixGetter  = denseMatrix
	_           matrixMutator = denseMatrix
)

type DenseRow []float64
type DenseMatrix struct {
	row, column int
	data        []DenseRow
}

func (m *DenseMatrix) Dims(trans bool) (int, int) {
	if !trans {
		return m.row, m.column
	} else {
		return m.column, m.row
	}
}

func (m *DenseMatrix) At(r, c int) float64 {
	if r < 0 || r >= m.row || c < 0 || c >= m.column {
		panic("Matrix index out of range.")
	}
	return m.data[r][c]
}

func (m *DenseMatrix) Row(r int) Matrix {
	if r < 0 || r >= m.row {
		panic("Matrix index out of range.")
	}
	return &DenseMatrix{
		row:    1,
		column: m.column,
		data:   m.data[r : r+1],
	}
}

// NOTE(baigang): Column shoud also return a matrix formated as one row?
func (m *DenseMatrix) Column(c int) Matrix {
	panic("")
}

func (m *DenseMatrix) SubMatrix(rs, cs, re, ce int) Matrix {
	panic("")
}

func (m *DenseMatrix) Set(r, c int, value float64) {
	if r < 0 || r >= m.row || c < 0 || c >= m.column {
		panic("Matrix index out of range.")
	}
	m.data[r][c] = value
}

func (m *DenseMatrix) Init(r, c int) Matrix {
	if r < 0 || c < 0 {
		panic("Matrix dimensions error.")
	}
	m.row = r
	m.column = c
	m.data = make([]DenseRow, r)
	// foreach row
	for i, _ := range m.data {
		m.data[i] = make([]float64, c)
	}
	return m
}

////////////////////////////////
var (
	sparseMatrix *SparseMatrix
	_            Matrix        = sparseMatrix
	_            matrixMetrics = sparseMatrix
	_            matrixGetter  = sparseMatrix
	_            matrixMutator = sparseMatrix
)

type SparseRow map[int]float64 // we assume the operation on a row is not concurrent
type SparseMatrix struct {
	row, column int
	data        []SparseRow
}

func (m *SparseMatrix) Dims(trans bool) (int, int) {
	if !trans {
		return m.row, m.column
	} else {
		return m.column, m.row
	}
}

func (m *SparseMatrix) At(r, c int) float64 {
	// NOTE: c doesn't mater here, so save the checking. Is this ok?
	if r < 0 || r >= m.row {
		panic("Matrix index out of range.")
	}
	value, ok := m.data[r][c]
	if ok {
		return value
	} else {
		return 0.0
	}
}

func (m *SparseMatrix) Row(r int) Matrix {
	if r < 0 || r >= m.row {
		panic("Matrix index out of range.")
	}
	return &SparseMatrix{
		row:    1,
		column: m.column,
		data:   m.data[r : r+1],
	}
}

func (m *SparseMatrix) Column(c int) Matrix {
	panic("")
}

func (m *SparseMatrix) SubMatrix(rs, cs, re, ce int) Matrix {
	panic("")
}

func (m *SparseMatrix) Set(r, c int, value float64) {
	if r < 0 || r >= m.row || c < 0 || c >= m.column {
		panic("Matrix index out of range.")
	}
	m.data[r][c] = value
}

func (m *SparseMatrix) Init(r, c int) Matrix {
	if r < 0 || c < 0 {
		panic("Matrix dimensions error.")
	}
	m.row = r
	m.column = c
	m.data = make([]SparseRow, r)
	// foreach row
	for i, _ := range m.data {
		m.data[i] = make(map[int]float64)
	}
	return m
}

// Matrix Ops

func Equals(A, B Matrix, transA, transB bool, epsilon float64) bool {
	rA, cA := A.Dims(transA)
	rB, cB := B.Dims(transB)
	if rA != rB || cA != cB {
		return false
	}

	// TODO parallel reduce

	return true
}

// C = alpha * A * B
func Mult(A, B, C Matrix, transA, transB bool, alpha float64) Matrix {
	rA, cA := A.Dims(transA)
	rB, cB := B.Dims(transB)
	if cA != rB {
		panic("Matrix A and B are not multiplicatable.")
	}
	rC, cC := C.Dims(false)
	if rC != rA || cC != cB {
		panic("Dimensions of matrix C is not compatible with AxB.")
	}

	var wg sync.WaitGroup
	for i := 0; i < rA; i++ {
		for j := 0; j < cB; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				var sum = 0.0
				for k := 0; k < cA; k++ {
					var va, vb float64
					if !transA {
						va = A.At(i, k)
					} else {
						va = A.At(k, i)
					}
					if !transB {
						vb = B.At(k, j)
					} else {
						vb = B.At(j, k)
					}
					sum += alpha * va * vb
				}
				C.Set(i, j, sum)
			}(i, j)
		}
	}
	wg.Wait()
	return C
}

// Calculating C = alpha*A + beta*B and return C
// NOTE:  C shouldn't be set as A or B
// NOTE:  C = alpha*X can be carried out by C = alpha*X+0*X
func LinearCombine(A, B, C Matrix, transA, transB bool, alpha, beta float64) Matrix {
	rA, cA := A.Dims(transA)
	rB, cB := B.Dims(transB)
	rC, cC := C.Dims(false)
	if rA != rB || rA != rC || cA != cB || cA != cC {
		panic("Matrix dimensions not compatible for adding.")
	}

	var wg sync.WaitGroup
	for i := 0; i < rC; i++ {
		for j := 0; j < cC; j++ {
			wg.Add(1)
			go func(i, j int) {
				defer wg.Done()
				var vA, vB float64
				if !transA {
					vA = A.At(i, j)
				} else {
					vA = A.At(j, i)
				}
				if !transB {
					vB = B.At(i, j)
				} else {
					vB = B.At(j, i)
				}
				C.Set(i, j, alpha*vA+beta*vB)
			}(i, j)
		}
	}
	wg.Wait()
	return C
}
