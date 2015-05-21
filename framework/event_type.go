package framework

import (
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type metaChange struct {
	from     uint64
	epoch    uint64
	linkType string
	meta     string
}

type dataRequest struct {
	ctx    context.Context
	taskID uint64
	epoch  uint64
	input  proto.Message
	method string
	retry  bool
}

type dataResponse struct {
	taskID uint64
	epoch  uint64
	method string
	input  proto.Message
	output proto.Message
}

type epochCheck struct {
	epoch   uint64
	resChan chan bool
}

func (c *epochCheck) fail() {
	c.resChan <- false
}

func (c *epochCheck) pass() {
	c.resChan <- true
}
