package regression

import (
	"golang.org/x/net/context"

	pb "github.com/taskgraph/taskgraph/example/regression/proto"
)

type regressionServer struct {
}

func (s *regressionServer) GetParameter(context.Context, *pb.Input) (*pb.Parameter, error) {
	panic("")
}
func (s *regressionServer) GetGradient(context.Context, *pb.Input) (*pb.Gradient, error) {
	panic("")
}
