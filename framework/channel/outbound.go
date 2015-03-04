package channel

import "github.com/taskgraph/taskgraph"

type outbound struct {
	channelCommon
	dataChan chan []byte
}

func (c *outbound) Put(m taskgraph.Marshaler) {
	data, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	c.dataChan <- data
}

func NewOutbound() *outbound {

}
