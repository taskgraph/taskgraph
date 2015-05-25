package bwmf

import (
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"testing"
)

const nRow = 2000
const nColumn = 1000
const nTopic = 10

func BenchmarkMatrix(b *testing.B) {
	dShard := pb.MatrixShard{Val: make([]float32, nRow*nTopic), M: nRow, N: nTopic}
	tShard := pb.MatrixShard{Val: make([]float32, nTopic*nColumn), M: nTopic, N: nColumn}
	W := Matrix{data: &dShard}
	H := Matrix{data: &tShard}

	for iter := 0; iter < b.N; iter++ {
		sum := float32(0.0)
		for i := uint32(0); i < nRow; i++ {
			for k := uint32(0); k < nTopic; k++ {
				for j := uint32(0); j < nColumn; j++ {
					sum += W.Get(i, k) * H.Get(k, j)
				}
			}
		}
	}
}

func BenchmarkMatrixRaw(b *testing.B) {
	wData := make([]float32, nRow*nTopic)
	tData := make([]float32, nTopic*nColumn)

	for iter := 0; iter < b.N; iter++ {
		sum := float32(0.0)
		for i := uint32(0); i < nRow; i++ {
			for k := uint32(0); k < nTopic; k++ {
				for j := uint32(0); j < nColumn; j++ {
					sum += wData[i*nTopic+k] * tData[k*nTopic+j]
				}
			}
		}
	}
}
