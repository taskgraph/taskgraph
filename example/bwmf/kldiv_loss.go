package bwmf

import (
	"taskgraph_op"
)

// `KLDivLoss` is a `Function` that evaluates Kullback-Leibler Divergence and the corresponding gradient at the given `Parameter`.
//
//  XXX(baigang): matrix layout
//    W is vectorized by the mapping W[ I, J ] = W_para[ I * k + J ]
//    H is vectorized by the mapping H[ I, J ] = H_para[ I * k + J ]
//  So actually W is W^T, but it saves code when alternatively optimize over H and W.
//
type KLDivLoss struct {
	V       *[]map[int]float32 // write once, read concurrently multiple times
	WH      *[][]float32       // temporary storage for the intermediate result W*H
	W       *Parameter
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
func (l *LKDivLoss) Evaluate(param Parameter, gradient Parameter) float32 {
	H := param
	accum := make(chan float32, 8)
	for i := 0; i < l.m; i++ {
		for j := 0; j < l.n; j++ {
			l.WH[i][j] = 0.0
			go func(i, j) {
				for k := 0; k < l.k; k++ {
					WH[i][j] += H.Get(i*l.k+k) * W.Get(j*l.k+k)
				}

				// evaluate element-wise KL-divergence
				v := V[i][j]
				wh := WH[i][j]
				var kl float32
				if wh > 1e-6 {
					accum <- v*log(v/wh) - v + wh
				} else {
					// XXX: kl for margin case. Should be 0 or very big value?
					accum <- 0.0
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
	for i := 0; i < l.m; i++ {
		for j := 0; j < l.k; j++ {
			wg.Add(1)
			grad_index = i*k + l
			gradient.set(grad_index, 0.0)
			go func(grad_index, i, j) {
				defer wg.Done()
				for k := 0; k < l.n; k++ {
					w_index := k*l.k + j
					grad_val := l.W.Get(w_index) * (WH[i][k] - V[i][k]) / WH[i][k]
					gradient.Add(grad_index, grad_val)
				}
			}(grad_index, i, j)
		}
	}
	wg.Wait()

	return value
}
