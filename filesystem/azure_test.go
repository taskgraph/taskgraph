package filesystem
import (
	"bytes"
	"io/ioutil"
	"testing"
	// "github.com/colinmarc/hdfs"
)




func setupAzureTest(t *testing.T) Client {
	if accountName == "" || accountKey == ""{
		t.Skip("Azure config not specified.")
	}
	client, err := NewAzureClient(namenodeAddr, webhdfsAddr, hdfsUser)
	if err != nil {
		t.Fatalf("NewHdfsClient(%s, %s) failed: %v",
			namenodeAddr, webhdfsAddr, err)
	}
	return client
}


func TestAzureClientWrite(t *testing.T) {
	client := setupAzureTest(t)
	writeCloser, err := client.OpenWriteCloser("tmptest01234567890123456789012345/testing")
	if err != nil {
		t.Fatalf("OpenWriteCloser failed: %v", err)
	}
	data := []byte("some data")
	_, err = writeCloser.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writeCloser.Close()
	readCloser, err := client.OpenReadCloser("tmptest01234567890123456789012345/testing")
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


// //Rename test

// 	err = cli.Rename("test1111111111111111111111111112/www2.txt", "test1111111111111111111111111112/b.txt")
// 	// err = cli.blobClient.CreateContainer("test1111111111111111111111111114", storage.ContainerAccessTypeBlob)
// 	if err != nil {
// 		// fmt.Println(err)
// 		fmt.Println(err)
// 	}