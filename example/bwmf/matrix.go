package bwmf

import (
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
)

type Matrix struct {
	data *pb.MatrixShard
}

// The use of following functions to access matrxies is NOT recommanded,
// as it will harm the performace given golang's limited inline support.
func (self *Matrix) Get(r, c uint32) float32 {
	return self.data.Val[r*self.data.N+c]
}

func (self *Matrix) Set(r, c uint32, v float32) {
	self.data.Val[r*self.data.N+c] = v
}

func (self *Matrix) M() uint32 {
	return self.data.M
}

func (self *Matrix) N() uint32 {
	return self.data.N
}
