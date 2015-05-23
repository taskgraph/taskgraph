package bwmf

import (
	"fmt"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	fs "github.com/taskgraph/taskgraph/filesystem"
)

func GetFsClient(config *Config) (fs.Client, error) {
	var client fs.Client
	var cltErr error
	switch config.IOConf.Fs {
	case "local":
		client = fs.NewLocalFSClient()
	case "hdfs":
		client, cltErr = fs.NewHdfsClient(
			config.IOConf.HdfsConf.NamenodeAddr,
			config.IOConf.HdfsConf.WebHdfsAddr,
			config.IOConf.HdfsConf.User,
		)
		if cltErr != nil {
			return nil, fmt.Errorf("Failed creating hdfs client %s", cltErr)
		}
	case "azure":
		client, cltErr = fs.NewAzureClient(
			config.IOConf.AzureConf.AccountName,
			config.IOConf.AzureConf.AccountKey,
			config.IOConf.AzureConf.BlogServiceBaseUrl,
			config.IOConf.AzureConf.ApiVersion,
			config.IOConf.AzureConf.UseHttps,
		)
		if cltErr != nil {
			return nil, fmt.Errorf("Failed creating azure client %s", cltErr)
		}
	default:
		return nil, fmt.Errorf("Unknow fs: %s", config.IOConf.Fs)
	}
	return client, nil
}

func LoadMatrixShard(client fs.Client, path string) (*pb.MatrixShard, error) {
	shard := &pb.MatrixShard{}
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
