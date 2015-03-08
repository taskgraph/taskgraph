package filesystem

import (
	"log"
	"os"
	"testing"
)

var _ *log.Logger

func TestHdfsClient(t *testing.T) {
	client, err := NewHdfsClient(os.Getenv("namenode_addr"), os.Getenv("webhdfs_addr"))
	// t.Skip()
	if err != nil {
		t.Fatalf("NewHdfsClient(%s, %s) failed: %v",
			os.Getenv("namenode_addr"), os.Getenv("webhdfs_url"), err)
	}
	writeCloser, err := client.OpenWriteCloser("/tmp/logs/testing")
	if err != nil {
		t.Fatalf("OpenWriteCloser failed: %v", err)
	}
	_, err = writeCloser.Write([]byte("heyhey"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writeCloser.Close()
}
