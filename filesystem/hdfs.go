package filesystem

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"

	"github.com/colinmarc/hdfs"
)

// Requirement:
//   hadoop version:
//   HDFS version:
//   REST API version:

type hdfsConfig struct {
	namenodeAddr string
	webHdfsAddr  string
	user         string
}

type HdfsClient struct {
	client *hdfs.Client
	hdfsConfig
}

func NewHdfsClient(namenodeAddr, webHdfsAddr, user string) (Client, error) {
	client, err := hdfs.NewForUser(namenodeAddr, user)
	if err != nil {
		return nil, err
	}
	// client.Remove("/tmp/logs/testing")
	return &HdfsClient{
		client: client,
		hdfsConfig: hdfsConfig{
			namenodeAddr: namenodeAddr,
			webHdfsAddr:  webHdfsAddr,
			user:         user,
		},
	}, nil
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
	return &HdfsFile{
		path:       name,
		logger:     log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags),
		hdfsConfig: c.hdfsConfig,
	}, nil
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
	path   string
	logger *log.Logger
	hdfsConfig
	// buffer
}

// REST docs:
// http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Append_to_a_File
func (f *HdfsFile) Write(b []byte) (int, error) {
	tr := &http.Transport{}
	urlStr := buildNamenodeURL(f.webHdfsAddr, f.path, f.user)
	f.logger.Printf("url: %s", urlStr)

	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		f.logger.Fatalf("NewRequest failed: %v", err)
	}
	// no redirect
	resp, err := tr.RoundTrip(req)
	if err != nil {
		f.logger.Fatalf("RoundTrip failed: %v", err)
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	f.logger.Printf("location: %s", loc)

	u, err := url.ParseRequestURI(loc)
	if err != nil {
		f.logger.Fatalf("ParseRequestURI failed: %v", err)
	}
	f.logger.Printf("data url: %s", u.String())
	resp, err = http.Post(u.String(), "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		f.logger.Fatalf("Post failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {

		f.logger.Fatalf("Status code isn't OK. Response: %v\nReason: %v", resp, explain(resp.Body))
	}
	return len(b), nil
}

func (f *HdfsFile) Close() error {
	return nil
}

func buildNamenodeURL(webHdfsAddr, name, user string) string {
	u := &url.URL{
		Scheme: "http",
		Host:   webHdfsAddr,
		Path:   path.Join("webhdfs", "v1", name),
	}
	q := u.Query()
	q.Set("op", "APPEND")
	q.Set("user.name", user)
	u.RawQuery = q.Encode()
	return u.String()
}

func explain(r io.Reader) interface{} {
	body, _ := ioutil.ReadAll(r)
	var reason interface{}
	json.Unmarshal(body, &reason)
	return reason
}
