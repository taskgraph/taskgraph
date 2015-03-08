package filesystem

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"

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

func (c *HdfsClient) OpenReadCloser(name string) (io.ReadCloser, error) {
	return c.client.Open(name)
}

func (c *HdfsClient) OpenWriteCloser(name string) (io.WriteCloser, error) {
	exist, err := c.Exists(name)
	if err != nil {
		return nil, err
	}
	if !exist {
		err := c.client.CreateEmptyFile(name)
		if err != nil {
			return nil, err
		}
	}
	return &HdfsFile{}, nil
}

func (c *HdfsClient) Exists(name string) (bool, error) {
	_, err := c.client.Stat(name)
	return existCommon(err)
}

func (c *HdfsClient) Rename(oldpath, newpath string) error {
	return c.client.Rename(oldpath, newpath)
}

func (c *HdfsClient) Glob(dirname, pattern string) ([]string, error) {
	fileInfoList, err := c.client.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, fi := range fileInfoList {
		matched, err := regexp.MatchString(pattern, fi.Name())
		if err != nil {
			return nil, err
		}
		if matched {
			res = append(res, fi.Name())
		}
	}
	return res, nil
}

type HdfsFile struct {
	logger *log.Logger
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

func (f *HdfsFile) Close() error {
	return nil
}
