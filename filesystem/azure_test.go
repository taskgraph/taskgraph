package filesystem

import (
	"bytes"
	"github.com/MSOpenTech/azure-sdk-for-go/storage"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

var (
	cnt, blob, accountName, accountKey, blobServiceBaseUrl, apiVersion string
	useHttps                                                           bool
)

// Example :
// accountName : yourAccountName
// accountKey : yourKey
// blobServiceBaseUrl : "core.chinacloudapi.cn"
// apiVersion : "2014-02-14"
// useHttps : true

func init() {
	accountName = os.Getenv("accountName")
	accountKey = os.Getenv("accountKey")
	blobServiceBaseUrl = os.Getenv("blobServiceBaseUrl")
	apiVersion = "2014-02-14"
	useHttps = true
	blob = "textForExamination"
}

func TestAzureClientWriteAndReadCloser(t *testing.T) {
	cli := setupAzureTest(t)
	cnt = randString(32)
	_, err := cli.blobClient.CreateContainerIfNotExists(cnt, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(cnt)

	writeCloser, err := cli.OpenWriteCloser(cnt + "/" + blob)
	if err != nil {
		t.Fatalf("OpenWriteCloser failed: %v", err)
	}
	defer cli.blobClient.DeleteBlob(cnt, blob)

	data := []byte("some data")
	_, err = writeCloser.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writeCloser.Close()

	readCloser, err := cli.OpenReadCloser(cnt + "/" + blob)
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

}

func TestAzureClientGlob(t *testing.T) {
	cli := setupAzureTest(t)
	cnt = randString(32)

	_, err := cli.blobClient.CreateContainerIfNotExists(cnt, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(cnt)

	err = cli.blobClient.PutBlockBlob(cnt, "1", strings.NewReader("Glob!"))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteBlob(cnt, blob)

	err = cli.blobClient.PutBlockBlob(cnt, "1.txt", strings.NewReader("Glob!"))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteBlob(cnt, blob)

	err = cli.blobClient.PutBlockBlob(cnt, "2.txt", strings.NewReader("Glob!"))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteBlob(cnt, blob)
	globPath := cnt + "/.*.txt"
	names, err := cli.Glob(globPath)
	if err != nil {
		t.Fatalf("Glob(%s) failed: %v", globPath, err)
	}
	// make sure glob result includes all *.txt files
	nameMap := make(map[string]int)
	for _, name := range names {
		nameMap[name] += 1
	}
	if len(names) != 2 ||
		nameMap[cnt+"/1.txt"] != 1 || nameMap[cnt+"/2.txt"] != 1 {
		t.Fatalf("Glob result isn't correct. Get = %v, Want = %v", nameMap, []string{"/tmp/testing/1.txt", "/tmp/testing/2.txt"})
	}
}

func TestAzureClientRename(t *testing.T) {
	cli := setupAzureTest(t)
	cnt = randString(32)
	_, err := cli.blobClient.CreateContainerIfNotExists(cnt, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(cnt)
	err = cli.blobClient.PutBlockBlob(cnt, blob, strings.NewReader("Rename!"))
	if err != nil {
		t.Fatal(err)
	}
	cli.Rename(cnt+"/"+blob, cnt+"/"+blob+"-Rename")
	exist, _ := cli.Exists(cnt + "/" + blob + "-Rename")
	if !exist {
		t.Fatalf("Rename failed")
	}
	exist, _ = cli.Exists(cnt + "/" + blob)
	if exist {
		t.Fatalf("Rename failed")
	}
	defer cli.blobClient.DeleteBlob(cnt, blob+"-Rename")
}

func TestAzureClientExist(t *testing.T) {
	cli := setupAzureTest(t)
	cnt = randString(32)
	_, err := cli.blobClient.CreateContainerIfNotExists(cnt, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(cnt)
	err = cli.blobClient.PutBlockBlob(cnt, blob, strings.NewReader("Exist!"))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteBlob(cnt, blob)
	ok, err := cli.Exists(cnt + "/" + blob + ".foo")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("Non-existing blob returned as existing: %s/%s", cnt, blob)
	}
	ok, err = cli.Exists(cnt + "/" + blob)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("Existing blob returned as non-existing: %s/%s", cnt, blob)
	}
}

func setupAzureTest(t *testing.T) *AzureClient {
	if accountName == "" || accountKey == "" || blobServiceBaseUrl == "" {
		t.Skip("Azure config not specified.")
	}
	client, err := NewAzureClient(accountName, accountKey, blobServiceBaseUrl, apiVersion, useHttps)
	if err != nil {
		t.Fatalf("NewAzureClient(%s, %s, %s) failed: %v",
			accountName, accountKey, blobServiceBaseUrl, err)
	}
	return client
}

func randString(n int) string {
	if n <= 0 {
		panic("negative number")
	}
	const alphanum = "0123456789abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}
