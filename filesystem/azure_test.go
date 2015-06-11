package filesystem

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/storage"
)

var (
	containerName, blobName, TestAzureAccountName, TestAzureAccountKey, TestAzureBlobServiceBaseUrl, apiVersion string
	useHttps                                                                                                    bool
)

var baseBlockID = base64.StdEncoding.EncodeToString([]byte("foo"))

// base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", 0)))

// Example :
// TestAzureAccountName : yourAccountName
// TestAzureAccountKey : yourKey
// TestAzureBlobServiceBaseUrl : "core.chinacloudapi.cn"
// apiVersion : "2014-02-14"
// useHttps : true

func init() {
	TestAzureAccountName = os.Getenv("TestAzureAccountName")
	TestAzureAccountKey = os.Getenv("TestAzureAccountKey")
	TestAzureBlobServiceBaseUrl = os.Getenv("TestAzureBlobServiceBaseUrl")
	apiVersion = "2014-02-14"
	useHttps = true
	blobName = "textforexamination"
}

func TestAzureClientWriteAndReadCloser(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(10)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(containerName)

	writeCloser, err := cli.OpenWriteCloser(containerName + "/" + blobName)
	if err != nil {
		t.Fatalf("OpenWriteCloser failed: %v", err)
	}
	defer cli.blobClient.DeleteBlob(containerName, blobName)

	data := []byte("some data")
	_, err = writeCloser.Write(data)
	_, err = writeCloser.Write(data)
	data = []byte("some datasome data")
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	writeCloser.Close()

	readCloser, err := cli.OpenReadCloser(containerName + "/" + blobName)
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

func TestAzureClientExistContainer(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(5)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := cli.Exists(containerName)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("Existing contianer returned as non-existing: %s/%s", containerName, blobName)
	}

	err = cli.blobClient.DeleteContainer(containerName)
	if err != nil {
		t.Fatal(err)
	}
	ok, err = cli.Exists(containerName)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("Non-existing contianer returned as existing: %s/%s", containerName, blobName)
	}
}

func TestAzureClientExistBlob(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(10)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(containerName)
	err = cli.blobClient.CreateBlockBlob(containerName, blobName)
	if err != nil {
		t.Fatal(err)
	}

	defer cli.blobClient.DeleteBlob(containerName, blobName)
	ok, err := cli.Exists(containerName + "/" + blobName + ".foo")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Errorf("Non-existing blob returned as existing: %s/%s", containerName, blobName)
	}
	ok, err = cli.Exists(containerName + "/" + blobName)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("!Existing blob returned as non-existing: %s/%s", containerName, blobName)
	}
}

func TestAzureClientRemoveBlob(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(32)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(containerName)
	err = cli.blobClient.CreateBlockBlob(containerName, blobName)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteBlob(containerName, blobName)

	cli.Remove(containerName + "/" + blobName)
	exist, err := cli.Exists(containerName + "/" + blobName)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatalf("Pointed blob removed failed")
	}
}

func TestAzureClientRemoveContainer(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(32)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(containerName)

	cli.Remove(containerName)
	exist, err := cli.Exists(containerName)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatalf("Pointed contianer removed failed")
	}
}

func TestAzureClientGlob(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(32)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(containerName)
	err = cli.blobClient.CreateBlockBlob(containerName, "1")
	if err != nil {
		t.Fatal(err)
	}

	defer cli.blobClient.DeleteBlob(containerName, blobName)
	err = cli.blobClient.CreateBlockBlob(containerName, "1.txt")
	if err != nil {
		t.Fatal(err)
	}

	defer cli.blobClient.DeleteBlob(containerName, blobName)
	err = cli.blobClient.CreateBlockBlob(containerName, "2.txt")
	if err != nil {
		t.Fatal(err)
	}

	defer cli.blobClient.DeleteBlob(containerName, blobName)
	globPath := containerName + "/*.txt"
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
		nameMap[containerName+"/1.txt"] != 1 || nameMap[containerName+"/2.txt"] != 1 {
		t.Fatalf("Glob result isn't correct. Get = %v, Want = %v", nameMap, []string{"/tmp/testing/1.txt", "/tmp/testing/2.txt"})
	}
}

func TestAzureClientRenameBlob(t *testing.T) {
	cli := setupAzureTest(t)
	containerName, err := randString(32)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cli.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(containerName)
	err = cli.blobClient.CreateBlockBlob(containerName, blobName)
	if err != nil {
		t.Fatal(err)
	}
	cli.Rename(containerName+"/"+blobName, containerName+"/"+blobName+"-Rename")
	exist, _ := cli.Exists(containerName + "/" + blobName + "-Rename")
	if !exist {
		t.Fatalf("Rename failed")
	}
	exist, err = cli.Exists(containerName + "/" + blobName)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatalf("Rename failed")
	}
	defer cli.blobClient.DeleteBlob(containerName, blobName+"-Rename")
}

func TestAzureClientRenameContainer(t *testing.T) {
	cli := setupAzureTest(t)
	srcContainerName, err := randString(32)
	if err != nil {
		t.Fatal(err)
	}

	dstContainerName, err := randString(32)
	if err != nil {
		t.Fatal(err)
	}
	blobName := "rename"
	_, err = cli.blobClient.CreateContainerIfNotExists(srcContainerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.blobClient.DeleteContainer(srcContainerName)
	err = cli.blobClient.CreateBlockBlob(srcContainerName, blobName+"01")
	if err != nil {
		t.Fatal(err)
	}
	err = cli.blobClient.CreateBlockBlob(srcContainerName, blobName+"02")
	if err != nil {
		t.Fatal(err)
	}

	cli.Rename(srcContainerName, dstContainerName)
	exist, _ := cli.Exists(dstContainerName + "/" + blobName + "01")
	if !exist {
		t.Fatalf("Rename failed")
	}
	exist, err = cli.Exists(dstContainerName + "/" + blobName + "02")
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Fatalf("Rename failed")
	}

	exist, err = cli.Exists(srcContainerName)
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatalf("Rename failed")
	}
	defer cli.blobClient.DeleteContainer(dstContainerName)
}

func setupAzureTest(t *testing.T) *AzureClient {
	if TestAzureAccountName == "" || TestAzureAccountKey == "" || TestAzureBlobServiceBaseUrl == "" {
		t.Skip("Azure config not specified.")
	}
	client, err := NewAzureClient(TestAzureAccountName, TestAzureAccountKey, TestAzureBlobServiceBaseUrl, apiVersion, useHttps)
	if err != nil {
		t.Fatalf("NewAzureClient(%s, %s, %s) failed: %v",
			TestAzureAccountName, TestAzureAccountKey, TestAzureBlobServiceBaseUrl, err)
	}
	return client
}

func randString(n int) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("negative number")
	}
	const alphanum = "0123456789abcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes), nil
}
