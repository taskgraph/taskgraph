package framework

type metaChange struct {
	from  uint64
	who   taskRole
	epoch uint64
	meta  string
}

type dataRequest struct {
	TaskID   uint64
	Epoch    uint64
	Req      string
	dataChan chan []byte
}

type dataResponse struct {
	TaskID uint64
	Epoch  uint64
	Req    string
	Data   []byte
}
