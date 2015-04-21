package op

// This defines how we do projected gradient.
type ProjectedGradient struct {
	projector          *Projection
	beta, sigma, alpha float32
}

type vgpair struct {
	value    float32
	gradient Parameter
}

func NewProjectedGradient(projector *Projection, beta, sigma, alpha float32) *ProjectedGradient {
	return &ProjectedGradient{
		projector: projector,
		beta:      beta,
		sigma:     sigma,
		alpha:     alpha,
	}
}

// This implementation is based on "Projected Gradient Methods for Non-negative Matrix
// Factorization" by Chih-Jen Lin. Particularly it is based on the discription of an
// improved projected gradient method in page 10 of that paper.
func (pg *ProjectedGradient) Minimize(loss Function, stop StopCriteria, vec Parameter) (float32, error) {

	stt := vec

	// Remember to clip the point before we do any thing.
	pg.projector.ClipPoint(stt)

	nxt := stt.CloneWithoutCopy()
	cdd := stt.CloneWithoutCopy()
	Fill(nxt, 0)

	// Evaluate once
	ovalgrad := &vgpair{value: 0, gradient: stt.CloneWithoutCopy()}
	evaluate(loss, stt, ovalgrad)
	nvalgrad := &vgpair{value: 0, gradient: stt.CloneWithoutCopy()}
	tvalgrad := &vgpair{value: 0, gradient: stt.CloneWithoutCopy()}

	alpha := pg.alpha

	for k := 0; !stop.Done(stt, ovalgrad.value, ovalgrad.gradient); k += 1 {

		newPoint(stt, nxt, ovalgrad.gradient, alpha, pg.projector)
		evaluate(loss, nxt, nvalgrad)

		if pg.isGoodStep(stt, nxt, ovalgrad, nvalgrad) {
			newPoint(stt, cdd, ovalgrad.gradient, alpha/pg.beta, pg.projector)
			evaluate(loss, cdd, tvalgrad)
			for pg.isGoodStep(stt, cdd, ovalgrad, tvalgrad) {
				nxt, cdd = cdd, nxt
				nvalgrad, tvalgrad = tvalgrad, nvalgrad
				// Now increase alpha as much as we can.
				alpha /= pg.beta
				newPoint(stt, cdd, ovalgrad.gradient, alpha/pg.beta, pg.projector)
				evaluate(loss, cdd, tvalgrad)
			}
		} else {
			// Now we decrease alpha barely enough to make sufficient decrease
			// of the objective value.
			for !pg.isGoodStep(stt, nxt, ovalgrad, nvalgrad) {
				alpha *= pg.beta
				newPoint(stt, nxt, ovalgrad.gradient, alpha, pg.projector)
				evaluate(loss, nxt, nvalgrad)
			}
		}

		// Now we arrive at a point satisfies sufficient decrease condition.
		// Swap the wts and gradient for the next round.
		stt, nxt = nxt, stt
		ovalgrad, nvalgrad = nvalgrad, ovalgrad
	}

	// Originally stt == vec, but stt may be swapped to newly created param (nxt). So copy it to output param here.
	for it := vec.IndexIterator(); it.Next(); {
		i := it.Index()
		vec.Set(i, stt.Get(i))
	}

	// This is so that we can reuse the step size in next round.
	pg.alpha = alpha

	// Return the final loss func value and error.
	return ovalgrad.value, nil
}

// This implements the sufficient decrease condition described in Eq (13)
func (pg *ProjectedGradient) isGoodStep(owts, nwts Parameter, ovg, nvg *vgpair) bool {
	valdiff := nvg.value - ovg.value
	sum := float64(0)
	for it := owts.IndexIterator(); it.Next(); {
		i := it.Index()
		sum += float64(ovg.gradient.Get(i) * (nwts.Get(i) - owts.Get(i)))
	}
	return valdiff <= pg.sigma*float32(sum)
}

// This creates a new point based on current point, step size and gradient.
func newPoint(owts, nwts, grad Parameter, alpha float32, projector *Projection) {
	for it := owts.IndexIterator(); it.Next(); {
		i := it.Index()
		nwts.Set(i, owts.Get(i)-alpha*grad.Get(i))
	}
	projector.ClipPoint(nwts)
}

func evaluate(loss Function, stt Parameter, ovalgrad *vgpair) {
	Fill(ovalgrad.gradient, 0)
	ovalgrad.value = loss.Evaluate(stt, ovalgrad.gradient)
}
