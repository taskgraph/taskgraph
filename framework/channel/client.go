package channel

import (
	pb "github.com/taskgraph/taskgraph/framework/channel/proto"
	"golang.org/x/net/context"
)

type client struct {
	inboundMap map[string][]*inbound
	grpcClient pb.ChannelClient
}

func (c *client) SetupDispatch() {
	for tag, inbounds := range c.inboundMap {
		go func(tag string, inbounds []*inbound) {
			data, err := c.grpcClient.GetData(context.Background(), &pb.Tag{tag})
			if err != nil {
				panic("")
			}
			for _, inbound := range inbounds {
				inbound.dataChan <- data.Payload
			}
		}(tag, inbounds)
	}
}

func (c *client) AttachInbound(in *inbound) {
	c.inboundMap[in.ID()] = append(c.inboundMap[in.ID()], in)
}

func NewClient() *client {

}
