package bwmf

import (
	"log"
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
	V       *pb.SparseMatrixShard
	W       op.Parameter // blockParameter. An op.Parameter wrapper with map[uint32]*DenseMatrixShard as underlying storage
	WH      [][]float32  // temporary storage for the intermediate result W*H
	m, n, k int          // dimensions
	smooth  float32
	logger  *log.Logger
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
	var value float32
	for i := 0; i < l.m; i++ {
		for j := 0; j < l.n; j++ {
			l.WH[i][j] = 0.0
			for k := 0; k < l.k; k++ {
				l.WH[i][j] += l.W.Get(i*l.k+k) * H.Get(j*l.k+k)
			}

			// evaluate element-wise KL-divergence
			v, ok := l.V.GetRow()[j].At[int32(i)]
			wh := l.WH[i][j]
			if ok {
				value += -v*float32(math.Log(float64(l.smooth+wh))) + wh
			} else {
				value += wh
			}
		}
	}
	// now, another pass for grad calculation
	for j := 0; j < l.n; j++ {
		for k := 0; k < l.k; k++ {
			grad_index := j*l.k + k
			gradient.Set(grad_index, 0.0)
			for i := 0; i < l.m; i++ {
				grad_val := l.W.Get(i*l.k+k) * (l.WH[i][j] - l.V.GetRow()[j].At[int32(i)]) / l.WH[i][j]
				gradient.Add(grad_index, grad_val)
			}
		}
	}

	if l.logger != nil {
		l.logger.Println("Evaluate at param ", param, " returns value ", value, " and gradient ", gradient)
	}

	return value
}
