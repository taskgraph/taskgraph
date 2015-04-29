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

	for i := uint32(0); i < M; i++ {
		for j := uint32(0); j < N; j++ {
			v := l.V.Get(j, i)
			wh := float32(0.0)
			for k := uint32(0); k < K; k += 1 {
				wh += l.W.Get(i,k) * H.Get(int(j*K+k))
			}
			// accumulate to grad vec

			if v != 0.0 {
				// v is non-zero
				value += -v*float32(math.Log(float64(wh+l.smooth))) + wh
				for k := uint32(0); k < K; k += 1 {
					gradient.Add(int(j*K+k), l.W.Get(i,k)*(1.0-(v+l.smooth)/(wh+l.smooth)))
				}
			} else {
				// v is zero
				value += wh

				for k := uint32(0); k < K; k += 1 {
					gradient.Add(int(j*K+k), l.W.Get(i,k))
				}
			}
		}
	}

	return value
}
