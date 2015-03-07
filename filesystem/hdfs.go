package filesystem

import (
	"path"
	"strings"

	"github.com/colinmarc/hdfs"
)

// Requirement:
//   hadoop version:
//   HDFS version:
//   REST API version:

type HdfsClient struct {
	client *hdfs.Client
}

func NewHdfsClient() Client {
	return &HdfsClient{}
}

func (c *HdfsClient) Create(name string) (File, error) {
	err := c.client.CreateEmptyFile(name)
	if err != nil {
		return nil, err
	}
	return c.Open(name)
}

func (c *HdfsClient) Open(name string) (File, error) {
	fileReader, err := c.client.Open(name)
	if err != nil {
		return nil, err
	}
	return &HdfsFile{
		FileReader: fileReader,
	}, nil
}

func (c *HdfsClient) Exists(name string) (bool, error) {
	_, err := c.client.Stat(name)
	return existCommon(err)
}

func (c *HdfsClient) Rename(oldpath, newpath string) error {
	return c.client.Rename(oldpath, newpath)
}

func (c *HdfsClient) GlobPrefix(prefix string) ([]string, error) {
	dirname := path.Dir(prefix)
	basename := path.Base(prefix)
	fileInfoList, err := c.client.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, fi := range fileInfoList {
		if strings.HasPrefix(fi.Name(), basename) {
			res = append(res, fi.Name())
		}
	}
	return res, nil
}

type HdfsFile struct {
	FileReader *hdfs.FileReader
}

func (f *HdfsFile) Read(b []byte) (int, error) {
	return f.FileReader.Read(b)
}
func (f *HdfsFile) Write(b []byte) (int, error) { panic("") }
func (f *HdfsFile) Sync() error                 { panic("") }

func (f *HdfsFile) Close() error {
	return f.FileReader.Close()
}
