package filesystem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
)

// Requirement:
//   Hadoop/HDFS version: 2
//   WebHDFS (HDFS REST)

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
	return &HdfsClient{
		client: client,
		hdfsConfig: hdfsConfig{
			namenodeAddr: namenodeAddr,
			webHdfsAddr:  webHdfsAddr,
			user:         user,
		},
	}, nil
}

func (c *HdfsClient) Remove(name string) error {
	return c.client.Remove(name)
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
	var (
		err error
		ret bool
	)
	for retry := 0; retry < 3; retry++ {
		_, err = c.client.Stat(name)
		ret, err = existCommon(err)
		if err == nil {
			return ret, err
		}
		time.Sleep(1 * time.Second)
		log.Printf("Recovering HDFS client with namenode %s for user %s. ret: %v", c.hdfsConfig.namenodeAddr, c.hdfsConfig.user, c.Recover())
	}
	return false, err
}

func (c *HdfsClient) Rename(oldpath, newpath string) error {
	return c.client.Rename(oldpath, newpath)
}

func (c *HdfsClient) Recover() error {
	var err error
	c.client, err = hdfs.NewForUser(c.hdfsConfig.namenodeAddr, c.hdfsConfig.user)
	return err
}

// only supports '*', '?'
// Syntax:
//    /user/hdfs/etl*/part.*
func (c *HdfsClient) Glob(pattern string) (matches []string, err error) {
	if pattern == "" {
		return nil, fmt.Errorf("Glob pattern shouldn't be empty")
	}
	if pattern[len(pattern)-1] == '/' {
		return nil, fmt.Errorf("Glob pattern shouldn't be a directory")
	}
	// names will have all the pathnames of the pattern.
	// e.g. "/a/b/c" => [a, b, c]
	var names []string
	for path.Dir(pattern) != "/" {
		names = append(names, path.Base(pattern))
		pattern = path.Dir(pattern)
	}
	names = append(names, pattern[1:len(pattern)])
	for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
		names[i], names[j] = names[j], names[i]
	}
	return c.glob("/", names)
}

func (c *HdfsClient) glob(dir string, names []string) (m []string, err error) {
	name := names[0]
	var dirs []string
	if hasMeta(name) {
		fileInfos, err := c.client.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, fi := range fileInfos {
			matched, err := path.Match(name, fi.Name())
			if err != nil {
				return nil, err
			}
			if matched {
				dirs = append(dirs, path.Join(dir, fi.Name()))
			}
		}
	} else {
		dirs = append(dirs, path.Join(dir, name))
	}
	for _, pathname := range dirs {
		if len(names) == 1 {
			exist, err := c.Exists(pathname)
			if err != nil {
				return nil, err
			}
			if exist {
				m = append(m, pathname)
			}
		} else {
			return c.glob(pathname, names[1:len(names)])

		}
	}
	return
}

func hasMeta(name string) bool {
	return strings.ContainsAny(name, "*?")
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

	// POST request to namenode. We shouldn't follow redirect and should get
	// the datanode URL in response.
	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		return 0, fmt.Errorf("Write: NewRequest failed: %v", err)
	}
	// no redirect

	var resp *http.Response
	var loc string
	for retry := 0; retry < 3 && (err != nil || loc == ""); retry++ {
		time.Sleep(300 * time.Millisecond)
		resp, err := tr.RoundTrip(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		loc = resp.Header.Get("Location")
	}

	if loc == "" {
		return 0, fmt.Errorf("Write: Failed retrieving datanode location.")
	}

	u, err := url.ParseRequestURI(loc)
	if err != nil {
		return 0, fmt.Errorf("Write: ParseRequestURI failed: %v", err)
	}
	// POST request to datanode.
	resp, err = http.Post(u.String(), "application/octet-stream", bytes.NewBuffer(b))
	for retry := 0; retry < 3 && err != nil; retry++ {
		time.Sleep(1 * time.Second)
		resp, err = http.Post(u.String(), "application/octet-stream", bytes.NewBuffer(b))
	}
	if err != nil {
		return 0, fmt.Errorf("Write: POST to datanode (%s) failed: %v", u.String(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Reason will show the java stack trace for the error.
		return 0, fmt.Errorf("Status code isn't OK. Response: %v\nReason: %v", resp, explain(resp.Body))
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
