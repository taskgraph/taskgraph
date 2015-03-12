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

func TestHdfsClientWrite(t *testing.T) {
	data := []byte("some data")
	checkHdfsConfig(t)
	client, err := NewHdfsClient(namenodeAddr, webhdfsAddr, hdfsUser)
	if err != nil {
		t.Fatalf("NewHdfsClient(%s, %s) failed: %v",
			namenodeAddr, webhdfsAddr, err)
	}
	writeCloser, err := client.OpenWriteCloser("/tmp/testing")
	if err != nil {
		t.Fatalf("OpenWriteCloser failed: %v", err)
	}
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

	if bytes.Compare(b, data) != 0 {
		t.Fatalf("Read result isn't correct. Get = %s, Want = %s", string(b), string(data))
	}

	c := client.(*HdfsClient)
	c.client.Remove("/tmp/testing")
}

func TestHdfsClientGlob(t *testing.T) {
	checkHdfsConfig(t)
	client, err := NewHdfsClient(namenodeAddr, webhdfsAddr, hdfsUser)
	if err != nil {
		t.Fatalf("NewHdfsClient(%s, %s) failed: %v",
			namenodeAddr, webhdfsAddr, err)
	}
	c := client.(*HdfsClient)
	c.client.Mkdir("/tmp/testing", 0644)
	c.client.CreateEmptyFile("/tmp/testing/1")
	c.client.CreateEmptyFile("/tmp/testing/1.txt")
	c.client.CreateEmptyFile("/tmp/testing/2.txt")

	globPath := "/tmp/testing/*.txt"
	names, err := client.Glob(globPath)
	if err != nil {
		t.Fatalf("Glob(%s) failed: %v", globPath, err)
	}
	nameMap := make(map[string]int)
	for _, name := range names {
		nameMap[name] += 1
	}

	if len(names) != 2 ||
		nameMap["/tmp/testing/1.txt"] != 1 || nameMap["/tmp/testing/2.txt"] != 1 {
		t.Fatalf("Glob result isn't correct. Get = %v, Want = %v", nameMap, []string{"/tmp/testing/1.txt", "/tmp/testing/2.txt"})
	}

	c.client.Remove("/tmp/testing")
}

func TestHdfsClientRename(t *testing.T) {
	checkHdfsConfig(t)

}

func checkHdfsConfig(t *testing.T) {
	if namenodeAddr == "" || webhdfsAddr == "" || hdfsUser == "" {
		t.Skip("HDFS config not specified.")
	}
}
