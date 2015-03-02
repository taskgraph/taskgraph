package bwmf

import (
	"math"
	"sync"

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
	V       []map[int]float32 // write once, read concurrently multiple times
	WH      [][]float32       // temporary storage for the intermediate result W*H
	W       taskgraph_op.Parameter
	m, n, k int // dimensions
}

// This function evaluates the Kullback-Leibler Divergence given $\mathbf{V} the matrix to fact and $\mathbf{W}$ the fixed factor.
//  The value is:
//
// $$ D_{KL} = \Sum_{ij} ( V_{ij} log \frac{V_{ij}}{(WH)_{ij}} - V_{ij} + (WH__{ij} )
//
//  The gradient is:
//
//  $$ \divsymb \frac{D_{KL}}{H} = -W*Z^T + W^T*\bar{Z} $$
//  , where $Z_{ij} = \frac{V_{ij}}{(WH)_{ij}}$
//
//  This implementation consists of two pass of visiting the full matrix, each of
//  which goes parallel. One pass is for evaluating W*H and accumulate kl-divergence
//  value and the other is for evalutating the matrix gradient of kl-div.
//
func (l *KLDivLoss) Evaluate(param taskgraph_op.Parameter, gradient taskgraph_op.Parameter) float32 {
	H := param
	accum := make(chan float32, 8)
	for i := 0; i < l.m; i++ {
		for j := 0; j < l.n; j++ {
			l.WH[i][j] = 0.0
			go func(i, j int) {
				for k := 0; k < l.k; k++ {
					l.WH[i][j] += l.W.Get(int64(i*l.k+k)) * H.Get(int64(j*l.k+k))
				}

				// evaluate element-wise KL-divergence
				v, ok := l.V[i][j]
				wh := l.WH[i][j]
				if ok {
					accum <- v*float32(math.Log(float64(v/wh))) - v + wh
				} else {
					accum <- wh
				}
			}(i, j)
		}
	}

	var value float32
	for c := 0; c < l.m*l.n; c++ {
		value += <-accum
	}

	// now another pass for grad calculation
	var wg sync.WaitGroup
	for i := 0; i < l.k; i++ {
		for j := 0; j < l.n; j++ {
			wg.Add(1)
			grad_index := int64(j*l.k + i)
			gradient.Set(grad_index, 0.0)
			go func(grad_index int64, i, j int) {
				defer wg.Done()
				for k := 0; k < l.m; k++ {
					w_index := int64(k*l.k + i)
					grad_val := l.W.Get(w_index) * (l.WH[k][j] - l.V[k][j]) / l.WH[k][j]
					gradient.Add(grad_index, grad_val)
				}
			}(grad_index, i, j)
		}
	}
	wg.Wait()

	return value
}
