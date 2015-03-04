package channel

type inbound struct {
	channelCommon
	dataChan chan []byte
}

func (c *inbound) Get() []byte {
	return <-c.dataChan
}

func NewInbound() *inbound {

}
