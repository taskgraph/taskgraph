package bwmf

import (
	"math"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/op"
)

// `KLDivLoss` is a `Function` that evaluates Kullback-Leibler Divergence and the corresponding gradient at the given `Parameter`.
//
//  XXX(baigang): matrix layout
//    W is vectorized by the mapping W[ I, J ] = W_para[ I * k + J ]
//    H is vectorized by the mapping H[ I, J ] = H_para[ I * k + J ]
//  So actually H is H^T, but it saves code by using identical routine when alternatively optimize over H and W.
//
type KLDivLoss struct {
	V       Matrix
	W       Matrix
	smooth  float32
}

func NewKLDivLoss(v *pb.MatrixShard, w []*pb.MatrixShard, m, n, k uint32, smooth float32) *KLDivLoss {
	wData := &pb.MatrixShard{M: m, N: k, Val: make([]float32, m*k)}
	idx := 0
	for idx = 0; idx < len(w); idx += 1 {
		w_ := w[idx]
		for i := uint32(0); i < w_.M * w_.N; i+=1 {
			wData.Val[idx] = w_.Val[i]
			idx += 1
		}
	}
	return &KLDivLoss{V: Matrix{data: v}, W: Matrix{data: wData}, smooth: smooth}
}

// This function evaluates the Kullback-Leibler Divergence given $\mathbf{V} the matrix to fact and $\mathbf{W}$ the fixed factor.
//  The generalized KL div is:
//
//    $$ D_{KL} = \Sum_{ij} ( V_{ij} log \frac{V_{ij}}{(WH)_{ij}} - V_{ij} + (WH_{ij} )
//
//  After removing the redundant constant factor and adding the smooth factor, it becomes:
//
//    $$ L_{kl} = \Sum{ij} ( -V_{ij} log((WH)_{ij} + smooth) + (WH)_{ij} )
//
//  The gradient is:
//
//  $$ \divsymb \frac{D_{KL}}{H} = -W^T*Z + W^T*\bar{Z} $$
//  , where $Z_{ij} = \frac{V_{ij}}{(WH)_{ij}}$ and \bar{Z}_{ij}=1
//
//  This implementation consists of two pass of visiting the full matrix, each of
//  which goes parallel. One pass is for evaluating W*H and accumulate kl-divergence
//  value and the other is for evalutating the matrix gradient of kl-div.
//
func (l *KLDivLoss) Evaluate(param op.Parameter, gradient op.Parameter) float32 {
	H := param
	op.Fill(gradient, 0.0)
	value := float32(0.0)

	M, N, K := l.W.M(), l.V.M(), l.W.N()
	H_data := H.Data()
	grad_data := gradient.Data()
	w_data := l.W.data.Val

	for i := uint32(0); i < M; i++ {
		for j := uint32(0); j < N; j++ {
			wh := float32(0.0)
			for k := uint32(0); k < K; k += 1 {
				wh += w_data[i*K+k] * H_data[j*K+k]
			}

			// v is zero
			value += wh
			for k := uint32(0); k < K; k += 1 {
				grad_data[j*K+k] += w_data[i*K+k]
			}
		}
	}

	for j := uint32(0); j < N; j++ {
		for p := l.V.data.Jc[j]; p < l.V.data.Jc[j+1]; p++ {
			i, v := l.V.data.Ir[p], l.V.data.Val[p]

			wh := float32(0.0)
			for k := uint32(0); k < K; k += 1 {
				wh += w_data[i*K+k] * H_data[j*K+k]
			}

			// accumulate to grad vec
			value += -v*float32(math.Log(float64(wh+l.smooth)))
			for k := uint32(0); k < K; k += 1 {
				grad_data[j*K+k] += -w_data[i*K+k]*(v+l.smooth)/(wh+l.smooth)
			}
		}
	}

	return value
}
