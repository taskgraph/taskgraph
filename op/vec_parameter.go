package op

// We need to implement a slice based parameter

type sizeIndexIterator struct {
	idx, size int
}

func (s *sizeIndexIterator) Index() int {
	return s.idx
}

func (s *sizeIndexIterator) Next() bool {
	s.idx += 1
	return s.idx < s.size
}

func (s *sizeIndexIterator) Rewind() {
	s.idx = -1
}

func (s *sizeIndexIterator) Size() int {
	return s.size
}

func MakeRangeIndexIterator(psize int) IndexIterator {
	return &sizeIndexIterator{idx: -1, size: psize}
}

type sliceParameter struct {
	param []float32
}

func (s *sliceParameter) Get(index int) float32 {
	return s.param[index]
}

func (s *sliceParameter) Set(index int, value float32) {
	s.param[index] = value
}

func (s *sliceParameter) Add(index int, value float32) {
	s.param[index] += value
}

// This allow us to generate parameter with same width.
func (s *sliceParameter) CloneWithoutCopy() Parameter {
	return &sliceParameter{param: make([]float32, len(s.param), len(s.param))}
}

// This allow one to enumerate through all parameters
func (s *sliceParameter) IndexIterator() IndexIterator {
	return &sizeIndexIterator{idx: -1, size: len(s.param)}
}

// This creates a new Vector based parameter
func NewVecParameter(size int) Parameter {
	return &sliceParameter{param: make([]float32, size, size)}
}
