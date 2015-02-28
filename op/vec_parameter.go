package taskgraph_op

// We need to implement a slice based parameter

type sizeIndexIterator struct {
	idx, size int64
}

func (s *sizeIndexIterator) Index() int64 {
	return s.idx
}

func (s *sizeIndexIterator) Next() bool {
	s.idx += 1
	return s.idx < s.size
}

func (s *sizeIndexIterator) Rewind() {
	s.idx = -1
}

type sliceParameter struct {
	param []float32
}

func (s *sliceParameter) Get(index int64) float32 {
	return s.param[index]
}

func (s *sliceParameter) Set(index int64, value float32) {
	s.param[index] = value
}

func (s *sliceParameter) Add(index int64, value float32) {
	s.param[index] += value
}

// This allow us to generate parameter with same width.
func (s *sliceParameter) CloneWithoutCopy() Parameter {
	return sliceParameter{param: make([]float32, len(s.param), len(s.param))}
}

// This allow one to enumerate through all parameters
func (s *sliceParameter) IndexIterator() IndexIterator {
	return sizeIndexIterator{idx: int64(-1), size: int64(len(s.param))}
}

// This creates a new Vector based parameter
func NewVecParameter(size int) Parameter {
	return sliceParameter{param: make([]float32, size, size)}
}
