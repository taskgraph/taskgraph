package framework

type context struct {
	epoch uint64
	f     *framework
}

func (f *framework) createContext() *context {
	return &context{
		epoch: f.epoch,
		f:     f,
	}
}

func (c *context) IncEpoch() {
	c.f.incEpoch(c.epoch)
}

func (c *context) FlagMeta(linkType, meta string) {
	c.f.flagMeta(linkType, meta, c.epoch)
}

func (c *context) DataRequest(toID uint64, req string) {
	c.f.dataRequest(toID, req, c.epoch)
}
