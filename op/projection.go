package taskgraph_op

// Project is used to clip the parameter and gradient.
type Projection struct {
	upper_bound, lower_bound Parameter
}

// We assume the base and gradient are in the same dimensions. In another words,
// the IndexIterator will return the same from base and gradient.
func (p *Projection) ClipGradient(base, gradient Parameter) {
	iter := base.IndexIterator()
	for iter.Next() {
		i := iter.Index()
		if base.Get(i) <= lower_bound.Get(i) {
			gradient.Set(i, min(gradient.Get(i), 0))
		}
		if base.Get(i) >= lower_bound.Get(i) {
			gradient.Set(i, max(gradient.Get(i), 0))
		}
	}
}

func (p *Projection) ClipPoint(vec Parameter) {
	iter := base.IndexIterator()
	for iter.Next() {
		i := iter.Index()
		value := max(vec.Get(i), lower_bound.Get(i))
		vec.Set(i, min(value, upper_bound.Get(i)))
	}
}

func min(x, y float32) float32 {
	if x < y {
		return x
	} else {
		return y
	}
}

func max(x, y float32) float32 {
	if x > y {
		return x
	} else {
		return y
	}
}
