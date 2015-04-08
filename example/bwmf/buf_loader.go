package bwmf

import (
	"fmt"
	"io/ioutil"

	"github.com/taskgraph/taskgraph/filesystem"
)

type BufLoader interface {
	Init() error
	ReadAll(path string) ([]byte, error)
}

type HdfsBufLoader struct {
	namenodeAddr string
	webHdfsAddr  string
	hdfsUser     string

	hdfsClient filesystem.Client
}

func NewHdfsBufLoader(namenodeAddr, webHdfsAddr, hdfsUser string) BufLoader {
	return &HdfsBufLoader{
		namenodeAddr: namenodeAddr,
		webHdfsAddr:  webHdfsAddr,
		hdfsUser:     hdfsUser,
	}
}

func (ld *HdfsBufLoader) Init() error {
	hdfsClient, hcOk := filesystem.NewHdfsClient(ld.namenodeAddr, ld.webHdfsAddr, ld.hdfsUser)
	if hcOk != nil {
		return fmt.Errorf("Failed connecting to HDFS at ", ld.namenodeAddr, ld.webHdfsAddr, ", with account ", ld.hdfsUser)
	}
	ld.hdfsClient = hdfsClient
	return nil
}

func (ld *HdfsBufLoader) ReadAll(path string) ([]byte, error) {
	return readAll(ld.hdfsClient, path)
}

type LocalBufLoader struct {
	lfsClient filesystem.Client
}

func NewLocalBufLoader() BufLoader {
	return &LocalBufLoader{}
}

func (ld *LocalBufLoader) Init() error {
	ld.lfsClient = filesystem.NewLocalFSClient()
	return nil
}

func (ld *LocalBufLoader) ReadAll(path string) ([]byte, error) {
	return readAll(ld.lfsClient, path)
}

func readAll(client filesystem.Client, path string) ([]byte, error) {
	reader, openOk := client.OpenReadCloser(path)
	if openOk != nil {
		return nil, fmt.Errorf("Failed open file in path %s with error %s.", path, openOk)
	}
	buf, readOk := ioutil.ReadAll(reader)
	if readOk != nil {
		return nil, fmt.Errorf("Failed reading file in path %s with error %s.", path, readOk)
	}
	return buf, nil
}
