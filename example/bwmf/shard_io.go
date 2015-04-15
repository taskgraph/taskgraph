package bwmf

import (
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	fs "github.com/taskgraph/taskgraph/filesystem"
)

func LoadSparseShard(client fs.Client, path string) (*pb.SparseMatrixShard, error) {
	shard := &pb.SparseMatrixShard{}
	lErr := loadMessage(client, path, shard)
	if lErr != nil {
		return nil, lErr
	}
	return shard, nil
}

func LoadDenseShard(client fs.Client, path string) (*pb.DenseMatrixShard, error) {
	shard := &pb.DenseMatrixShard{}
	lErr := loadMessage(client, path, shard)
	if lErr != nil {
		return nil, lErr
	}
	return shard, nil
}

func SaveSparseShard(client fs.Client, shard *pb.SparseMatrixShard, path string) error {
	return saveMessage(client, shard, path)
}

func SaveDenseShard(client fs.Client, shard *pb.DenseMatrixShard, path string) error {
	return saveMessage(client, shard, path)
}

func loadMessage(client fs.Client, path string, message proto.Message) error {
	reader, cErr := client.OpenReadCloser(path)
	if cErr != nil {
		return cErr
	}
	buf, rdErr := ioutil.ReadAll(reader)
	if rdErr != nil {
		return rdErr
	}
	deErr := fromByte(buf, message)
	if deErr != nil {
		return deErr
	}
	return nil
}

func saveMessage(client fs.Client, msg proto.Message, path string) error {
	buf, seErr := toByte(msg)
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
