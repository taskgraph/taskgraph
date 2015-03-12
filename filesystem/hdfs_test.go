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
	t.Skip()
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
}

func TestHdfsClientGlob(t *testing.T) {
	checkHdfsConfig(t)
	client, err := NewHdfsClient(namenodeAddr, webhdfsAddr, hdfsUser)
	if err != nil {
		t.Fatalf("NewHdfsClient(%s, %s) failed: %v",
			namenodeAddr, webhdfsAddr, err)
	}
	globPath := "/tmp/*"
	names, err := client.Glob(globPath)
	if err != nil {
		t.Fatalf("Glob(%s) failed: %v", globPath, err)
	}
	fmt.Println(names)
}

func checkHdfsConfig(t *testing.T) {
	if namenodeAddr == "" || webhdfsAddr == "" || hdfsUser == "" {
		t.Skip("HDFS config not specified.")
	}
}
