package channel

import (
	pb "github.com/taskgraph/taskgraph/framework/channel/proto"
	"golang.org/x/net/context"
)

type server struct {
	outboundMap map[string]*outbound
}

func (s *server) GetData(ctx context.Context, tag *pb.Tag) (*pb.Data, error) {
	outbound := s.outboundMap[tag.Name]
	data := <-outbound.dataChan
	return &pb.Data{data}, nil
}

func (s *server) AttachOutbound(out *outbound) {
	s.outboundMap[out.ID()] = out
}

func NewServer() *server {

}
