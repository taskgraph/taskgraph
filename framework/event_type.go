package framework

type metaChange struct {
	from  uint64
	who   taskRole
	epoch uint64
	meta  string
}

type dataReqToSend struct {
	to    uint64
	req   string
	epoch uint64
}
