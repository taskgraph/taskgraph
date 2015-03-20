package framework

import "github.com/golang/protobuf/proto"

type metaChange struct {
	from  uint64
	who   string
	epoch uint64
	meta  string
}

type dataRequest struct {
	taskID   uint64
	epoch    uint64
	linkType string
	input    proto.Message
	method   string
	retry    bool
}

type dataResponse struct {
	taskID   uint64
	epoch    uint64
	linkType string
	input    proto.Message
	output   proto.Message
}

type epochCheck struct {
	epoch uint64
}
