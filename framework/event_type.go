package framework

type metaChange struct {
	from  uint64
	who   string
	epoch uint64
	meta  string
}

type dataRequest struct {
	epoch          uint64
	epochMismatchC chan struct{}
	epochCheckedC  chan struct{}
}

func (dr *dataRequest) epochMismatch() {
	close(dr.epochMismatchC)
}

func (dr *dataRequest) send() {
	close(dr.epochCheckedC)
}

type dataResponse struct {
	epoch          uint64
	epochMismatchC chan struct{}
	epochCheckedC  chan struct{}
}

func (dr *dataResponse) epochMismatch() {
	close(dr.epochMismatchC)
}

func (dr *dataResponse) finish() {
	close(dr.epochCheckedC)
}
