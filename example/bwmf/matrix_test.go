package bwmf

import (
	"testing"
)

func TestDense(t *testing.T) {
	var mat DenseMatrix
	testMatrix(t, &mat)
}

func TestSparse(t *testing.T) {
	var mat SparseMatrix
	testMatrix(t, &mat)
}

func TestDDOps(t *testing.T) {
	var m1, m2 DenseMatrix
	testOperations(t, &m1, &m2)
}

func TestDSOps(t *testing.T) {
	var m1 DenseMatrix
	var m2 SparseMatrix
	testOperations(t, &m1, &m2)
}

func TestSSOps(t *testing.T) {
	var m1, m2 SparseMatrix
	testOperations(t, &m1, &m2)
}

func TestSDOps(t *testing.T) {
	var m1 SparseMatrix
	var m2 DenseMatrix
	testOperations(t, &m1, &m2)
}

func testMatrix(t *testing.T, mat Matrix) {

	mat.Init(3, 4)
	row, column := mat.Dims(false)
	if row != 3 || column != 4 {
		t.Errorf("Matrix.Dims. row: expected 3, actual: %d; column: expected 4, actual %d.", row, column)
	}

	mat.Set(0, 0, 1.0)
	mat.Set(1, 1, 1.0)
	mat.Set(2, 2, 1.0)
	mat.Set(2, 3, 2.0)

	m00 := mat.At(0, 0)
	m01 := mat.At(0, 1)
	m23 := mat.At(2, 3)

	if m00 != 1.0 || m01 != 0.0 || m23 != 2.0 {
		t.Errorf("Matrix.At m00: expected 1.0, actual: %f; m01: expected 0.0, actual %f; m23: expected 2.0, actual: %f.", m00, m01, m23)
	}

	rowMat := mat.Row(2)

	r0, r1, r2, r3 := rowMat.At(0, 0), rowMat.At(0, 1), rowMat.At(0, 2), rowMat.At(0, 3)

	if r0 != 0.0 || r1 != 0.0 || r2 != 1.0 || r3 != 2.0 {
		t.Errorf("Matrix.Row r0: expected ")
	}
}

func testOperations(t *testing.T, m1, m2 Matrix) {
	var m3 DenseMatrix

	// Matrix Product
	m1.Init(3, 2)
	m2.Init(2, 3)
	m3.Init(3, 3)

	// m1 is set to be:
	// |  3 6 |
	// |  1 4 |
	// | -1 2 |
	for i := 0; i < 3; i++ {
		for j := 0; j < 2; j++ {
			m1.Set(i, j, 3.0-float64(2*i+3*j))
		}
	}

	// m2 is set to be:
	// | 7  9  11 |
	// | 3  5   7 |
	for i := 0; i < 2; i++ {
		for j := 0; j < 3; j++ {
			m2.Set(i, j, 7.0-float64(4*i+2*j))
		}
	}

	Mult(m1, m2, &m3, false, false, 1.0)

	// so m3 should be:
	// | 39  57  75  |
	// | 19  29  39  |
	// | -1  -1   3  |

	var m4 DenseMatrix
	m4.Init(3, 3)
	m4.Set(0, 0, 39.0)
	m4.Set(0, 0, 57.0)
	m4.Set(0, 0, 75.0)
	m4.Set(0, 0, 19.0)
	m4.Set(0, 0, 29.0)
	m4.Set(0, 0, 39.0)
	m4.Set(0, 0, -1.0)
	m4.Set(0, 0, -1.0)
	m4.Set(0, 0, 3.0)

	if !Equals(&m3, &m4, false, false, 1e-6) {
		t.Errorf("Matrix Mult test failed.")
	}

	// TODO

	// Matrix Scale
	// TODO

	// Matrix Element Product
	// TODO

	// Matrix Add
	// TODO

	// Matrix Subtract
	// TODO

}
