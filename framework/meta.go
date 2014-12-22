package framework

type metaChange struct {
	from  uint64
	who   taskRole
	epoch uint64
	meta  string
}
