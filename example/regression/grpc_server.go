package regression

import (
	"golang.org/x/net/context"

	pb "github.com/taskgraph/taskgraph/example/regression/proto"
)

type regressionServer struct {
	param    *pb.Parameter
	gradient *pb.Gradient
}

func (s *regressionServer) GetParameter(context.Context, *pb.Input) (*pb.Parameter, error) {
	return s.param, nil
}
func (s *regressionServer) GetGradient(context.Context, *pb.Input) (*pb.Gradient, error) {
	return s.gradient, nil
}
