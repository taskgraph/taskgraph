package framework

type metaChange struct {
	from  uint64
	who   taskRole
	epoch uint64
	meta  string
}

type dataRequest struct {
	taskID   uint64
	epoch    uint64
	req      string
	dataChan chan []byte
}

func (dr *dataRequest) notifyEpochMismatch() {
	close(dr.dataChan)
}

type dataResponse struct {
	taskID   uint64
	epoch    uint64
	req      string
	data     []byte
	dataChan chan []byte
}

func (dr *dataResponse) notifyEpochMismatch() {
	close(dr.dataChan)
}
