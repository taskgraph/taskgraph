package bwmf

import (
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	fs "github.com/taskgraph/taskgraph/filesystem"
)

func LoadMatrixShard(client fs.Client, path string) (*pb.MatrixShard, error) {
	shard := &pb.MatrixShard{}

	// Force reconnecting. This is fixing the issue that the client was created long time ago and the conn might have broken.
	client.Recover()
	reader, cErr := client.OpenReadCloser(path)
	if cErr != nil {
		return nil, cErr
	}
	buf, rdErr := ioutil.ReadAll(reader)
	if rdErr != nil {
		return nil, rdErr
	}
	rdErr = fromByte(buf, shard)
	if rdErr != nil {
		return nil, rdErr
	}
	return shard, nil
}

func SaveMatrixShard(client fs.Client, shard *pb.MatrixShard, path string) error {
	buf, seErr := toByte(shard)
	if seErr != nil {
		return seErr
	}
	writer, oErr := client.OpenWriteCloser(path)
	if oErr != nil {
		return oErr
	}
	_, wErr := writer.Write(buf)
	return wErr
}

func toByte(msg proto.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

func fromByte(buf []byte, message proto.Message) error {
	unmarshErr := proto.Unmarshal(buf, message)
	if unmarshErr != nil {
		return unmarshErr
	}
	return nil
}
