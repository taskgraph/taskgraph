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
	taskID   uint64
	epoch    uint64
	req      string
	data     []byte
	dataChan chan []byte
}
