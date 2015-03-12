package filesystem

import (
	"fmt"
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
	_, err = writeCloser.Write([]byte("some data"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writeCloser.Close()

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
	fmt.Println(names)

	c.client.Remove("/tmp/testing")
}

func checkHdfsConfig(t *testing.T) {
	if namenodeAddr == "" || webhdfsAddr == "" || hdfsUser == "" {
		t.Skip("HDFS config not specified.")
	}
}
