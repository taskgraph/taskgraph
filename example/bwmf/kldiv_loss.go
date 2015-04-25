package bwmf

import (
	"math"
	"sort"

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
	V       *pb.MatrixShard
	W       []*pb.MatrixShard
	m, n, k int // dimensions
	smooth  float32

	ends []int // for binary search of blockId.
}

func NewKLDivLoss(v *pb.MatrixShard, w []*pb.MatrixShard, m, n, k int, smooth float32) *KLDivLoss {
	stt := make([]int, len(w)+1)
	ost := 0
	for i, m := range w {
		stt[i] = ost
		ost += len(m.Row)
	}
	stt[len(w)] = ost
	return &KLDivLoss{V: v, W: w, m: m, n: n, k: k, smooth: smooth, ends: stt}
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
	wi := make([]float32, l.k)
	for i := 0; i < l.m; i++ {
		for j := 0; j < l.n; j++ {
			v, _ := l.V.GetRow()[j].At[int32(i)]
			wh := l.smooth // move away from 0

			for k := 0; k < l.k; k++ {
				w := l.GetWElem(i, k)
				wi[k] = w
				wh += w * H.Get(j*l.k+k)
			}

			// evaluate element-wise KL-divergence
			value += -v*float32(math.Log(float64(wh))) + wh

			// accumulate to grad vec
			for k := 0; k < l.k; k++ {
				gradient.Add(j*l.k+k, wi[k]*(wh-v)/wh)
			}
		}
	}
	return value
}

func (l *KLDivLoss) GetWElem(row, column int) float32 {
	b := sort.Search(len(l.ends), func(i int) bool { return l.ends[i] > row }) - 1
	return l.W[b].Row[row-l.ends[b]].At[int32(column)]
}
