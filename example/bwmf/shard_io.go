package bwmf

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
)

func LoadSparseShard(conf ioconfig, path string) (*pb.SparseMatrixShard, error) {
	shard := &pb.SparseMatrixShard{}
	lErr := loadMessage(conf, path, shard)
	if lErr != nil {
		return nil, lErr
	}
	return shard, nil
}

func LoadDenseShard(conf ioconfig, path string) (*pb.DenseMatrixShard, error) {
	shard := &pb.DenseMatrixShard{}
	lErr := loadMessage(conf, path, shard)
	if lErr != nil {
		return nil, lErr
	}
	return shard, nil
}

func SaveSparseShard(conf ioconfig, shard *pb.SparseMatrixShard, path string) error {
	return saveMessage(conf, shard, path)
}

func SaveDenseShard(conf ioconfig, shard *pb.DenseMatrixShard, path string) error {
	return saveMessage(conf, shard, path)
}

func loadMessage(conf ioconfig, path string, message proto.Message) error {
	reader, cErr := createReader(conf, path)
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

func saveMessage(conf ioconfig, msg proto.Message, path string) error {
	buf, seErr := toByte(msg)
	if seErr != nil {
		return seErr
	}
	writer, oErr := createWriter(conf, path)
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

func createReader(conf ioconfig, path string) (io.ReadCloser, error) {
	switch conf.IFs {
	case "local":
		return filesystem.NewLocalFSClient().OpenReadCloser(path)
	case "hdfs":
		client, cltErr := filesystem.NewHdfsClient(conf.HdfsConf.NamenodeAddr, conf.HdfsConf.WebHdfsAddr, conf.HdfsConf.User)
		if cltErr != nil {
			return nil, cltErr
		}
		return client.OpenReadCloser(path)
	case "azure":
		client, cltErr := filesystem.NewAzureClient(conf.AzureConf.AccountName, conf.AzureConf.AccountKey, conf.AzureConf.BlogServiceBaseUrl,
			conf.AzureConf.ApiVersion, conf.AzureConf.UseHttps)
		if cltErr != nil {
			return nil, cltErr
		}
		return client.OpenReadCloser(path)
	default:
		return nil, fmt.Errorf("Unknow fs: %s", conf.IFs)
	}
}

func createWriter(conf ioconfig, path string) (io.WriteCloser, error) {
	switch conf.OFs {
	case "local":
		return filesystem.NewLocalFSClient().OpenWriteCloser(path)
	case "hdfs":
		client, cltErr := filesystem.NewHdfsClient(conf.HdfsConf.NamenodeAddr, conf.HdfsConf.WebHdfsAddr, conf.HdfsConf.User)
		if cltErr != nil {
			return nil, cltErr
		}
		return client.OpenWriteCloser(path)
	case "azure":
		client, cltErr := filesystem.NewAzureClient(conf.AzureConf.AccountName, conf.AzureConf.AccountKey, conf.AzureConf.BlogServiceBaseUrl,
			conf.AzureConf.ApiVersion, conf.AzureConf.UseHttps)
		if cltErr != nil {
			return nil, cltErr
		}
		return client.OpenWriteCloser(path)
	default:
		return nil, fmt.Errorf("Unknow fs: %s", conf.IFs)
	}
}
