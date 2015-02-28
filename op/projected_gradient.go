package taskgraph_op

// This defines how we do projected gradient.
type ProjectedGradient struct {
	projector          Projection
	beta, sigma, alpha float32
}

type vgpair struct {
	value    float32
	gradient Parameter
}

// This implementation is based on "Projected Gradient Methods for Non-negative Matrix
// Factorization" by Chih-Jen Lin. Particularly it is based on the discription of an
// improved projected gradient method in page 10 of that paper.
func (pg ProjectedGradient) Minimize(loss Function, stop StopCriteria, vec Parameter) {

	stt := &vec

	// Remember to clip the point before we do any thing.
	projector.ClipPoint(stt)

	nxt := &stt.CloneWithoutCopy()
	Fill(nxt, 0)

	// Evaluate once
	ovalgrad := &vgpair{value: 0, gradient: stt.CloneWithoutCopy()}

	evaluate(ovalgrad, loss)

	nvalgrad := &vgpair{value: 0, gradient: stt.CloneWithoutCopy()}

	// Take this outside to save function evaluation
	projector.ClipGradient(stt, ovalgrad.gradient)
	alpha_moves := make([]float32, 5, 5)
	current_alpha := alpha
	for k := 0; !stop.Done(stt, ovalgrad.value, ovalgrad.gradient); k += 1 {

		alpha_moves[k%alpha_moves.size()] = 0
		newPoint(stt, nxt, ovalgrad.gradient, alpha, projector)

		evaluate(nvalgrad, loss)
		if isGoodStep(stt, nxt, ovalgrad, nvalgrad) {
			alpha_moves[k%alpha_moves.size()] = 1
			// Now increase alpha as much as we can.
			move_sum := 0
			for l := 0; l < alpha_moves.size(); l += 1 {
				move_sum += alpha_moves[l]
			}
			if move_sum > 1 {
				alpha /= beta_
				for index, _ := range alpha_moves {
					alpha_moves[index] = 0
				}
			}
		} else {
			// Now we decrease alpha barely enough to make sufficient decrease
			// of the objective value.
			dec_count := 0
			for isGoodStep(stt, nxt, ovalgrad, nvalgrad) {
				alpha *= beta_
				dec_count += 1
				newPoint(stt, nxt, ovalgrad.gradient, alpha, projector)
				evaluate(nvalgrad, loss)
			}
			alpha_moves[k%alpha_moves.size()] = -dec_count
		}

		// Make sure we preserve the alpha value for next round.
		current_alpha = alpha

		// Swap the wts and gradient for the next round.
		{
			tmp := stt
			stt = nxt
			nxt = tmp
		}

		{
			tmp := ovalgrad
			ovalgrad = nvalgrad
			nvalgrad = tmp
		}

		projector.ClipGradient(stt, ovalgrad.gradient)
	}

	// This is so that we can reuse the step size in next round.
	alpha_ = current_alpha

	// Simply return true to indicate the minimization is done.
	return true
}

func isGoodStep(owts, nwts *Parameter, ovg, nvg *vgpair) bool {
	valdiff := nvg.value - ovg.value
	sum := float64(0)
	for it := owts.IndexIterator(); it.Next(); {
		i := it.Index()
		sum += float64(ovg.gradient.Get(i) * (nwts.Get(i) - owts.Get(i)))
	}
	return valdiff <= sigma_*sum
}

// This creates a new point based on current point, step size and gradient.
func newPoint(owts, nwts, gradient Parameter, alpha float32, projector *Projection) {
	for it := owts.IndexIterator(); it.Next(); {
		nwts.Set(i, owts.Get(i)-alpha*grad.Get(i))
	}
	projector.ClipPoint(nwts)
}

func evalue(loss Function, ovalgrad *vapair) {
	Fill(ovalgrad.gradient, 0)
	ovalgrad.value = loss.Evaluate(stt, ovalgrad.gradient)
}
