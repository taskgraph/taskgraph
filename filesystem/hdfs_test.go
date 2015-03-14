package filesystem

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

var (
	namenodeAddr, webhdfsAddr, hdfsUser string
)

func init() {
	namenodeAddr = os.Getenv("namenode_addr")
	webhdfsAddr = os.Getenv("webhdfs_addr")
	hdfsUser = os.Getenv("hdfs_user")
}

// I was making two assumptions through all test cases:
// 1. "/tmp" dir exists.
// 2. I don't care about test failure. Though under failures you might need
//    to clean up files or dirs manually.

func TestHdfsClientWrite(t *testing.T) {
	client := setupHdfsTest(t)
	writeCloser, err := client.OpenWriteCloser("/tmp/testing")
	if err != nil {
		t.Fatalf("OpenWriteCloser failed: %v", err)
	}
	data := []byte("some data")
	_, err = writeCloser.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writeCloser.Close()
	readCloser, err := client.OpenReadCloser("/tmp/testing")
	if err != nil {
		t.Fatalf("OpenReadCloser failed: %v", err)
	}
	b, err := ioutil.ReadAll(readCloser)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	readCloser.Close()

	if bytes.Compare(b, data) != 0 {
		t.Fatalf("Read result isn't correct. Get = %s, Want = %s", string(b), string(data))
	}

	c := client.(*HdfsClient).client
	c.Remove("/tmp/testing")
}

func TestHdfsClientGlob(t *testing.T) {
	client := setupHdfsTest(t)

	c := client.(*HdfsClient).client
	c.Mkdir("/tmp/testing", 0644)
	c.CreateEmptyFile("/tmp/testing/1")
	c.CreateEmptyFile("/tmp/testing/1.txt")
	c.CreateEmptyFile("/tmp/testing/2.txt")
	defer c.Remove("/tmp/testing")

	globPath := "/tmp/testing/*.txt"
	names, err := client.Glob(globPath)
	if err != nil {
		t.Fatalf("Glob(%s) failed: %v", globPath, err)
	}
	// make sure glob result includes all *.txt files
	nameMap := make(map[string]int)
	for _, name := range names {
		nameMap[name] += 1
	}
	if len(names) != 2 ||
		nameMap["/tmp/testing/1.txt"] != 1 || nameMap["/tmp/testing/2.txt"] != 1 {
		t.Fatalf("Glob result isn't correct. Get = %v, Want = %v", nameMap, []string{"/tmp/testing/1.txt", "/tmp/testing/2.txt"})
	}
}

func TestHdfsClientRename(t *testing.T) {
	client := setupHdfsTest(t)

	c := client.(*HdfsClient).client
	c.CreateEmptyFile("/tmp/testing")

	client.Rename("/tmp/testing", "/tmp/tesing-renamed")
	exist, _ := client.Exists("/tmp/tesing-renamed")
	if !exist {
		t.Fatalf("Rename failed")
	}
	c.Remove("/tmp/renamed")
}

func setupHdfsTest(t *testing.T) Client {
	if namenodeAddr == "" || webhdfsAddr == "" || hdfsUser == "" {
		t.Skip("HDFS config not specified.")
	}
	client, err := NewHdfsClient(namenodeAddr, webhdfsAddr, hdfsUser)
	if err != nil {
		t.Fatalf("NewHdfsClient(%s, %s) failed: %v",
			namenodeAddr, webhdfsAddr, err)
	}
	return client
}
