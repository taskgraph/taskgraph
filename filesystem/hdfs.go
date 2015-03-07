package filesystem

import (
	"bytes"
	"log"
	"net/http"
	"net/url"
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
	logger     *log.Logger
}

func (f *HdfsFile) Read(b []byte) (int, error) {
	return f.FileReader.Read(b)
}

// REST docs:
// http://hadoop.apache.org/docs/r2.5.1/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File
func (f *HdfsFile) Write(b []byte) (int, error) {
	tr := &http.Transport{}
	urlStr := ""
	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		f.logger.Fatalf("NewRequest failed: %v", err)
	}
	// no redirect
	resp, err := tr.RoundTrip(req)
	if err != nil {
		f.logger.Fatalf("RoundTrip failed: %v", err)
	}
	loc := resp.Header.Get("Location")
	u, err := url.ParseRequestURI(loc)
	if err != nil {
		f.logger.Fatalf("ParseRequestURI failed: %v", err)
	}
	resp, err = http.Post(u.String(), "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		f.logger.Fatalf("Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		f.logger.Fatalf("Status code isn't OK")
	}
	return len(b), nil
}

func (f *HdfsFile) Sync() error {
	return nil
}

func (f *HdfsFile) Close() error {
	return f.FileReader.Close()
}
