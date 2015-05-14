// TODO : updated the semantics of the Azure filesystem
// Explanation :
// current semantics is Container/Blob like "A/B", restricted by only one slash
// Might need to update the senmatics supported multiple slash
// (As same as the local system semantic)
// "/A/B/C/D", ignore the first slash, "A" represents the contianer name
// and "B/C/D" represents the Blob name.
// Correspendingly, the Blob function, Remove function,
// Exist function, Rename function need to change.

package filesystem

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
)

type AzureClient struct {
	client     *storage.Client
	blobClient *storage.BlobStorageClient
}

type AzureFile struct {
	path   string
	logger *log.Logger
	client *storage.BlobStorageClient
}

// convertToAzurePath function
// convertToAzurePath splits the given name into two parts
// The first part represents the container's name, and the length of it shoulb be 32 due to Azure restriction
// The second part represents the blob's name
// It will return any error while converting
func convertToAzurePath(name string) (string, string, error) {
	afterSplit := strings.Split(name, "/")
	if len(afterSplit[0]) != 32 {
		return "", "", fmt.Errorf("azureClient : the length of container should be 32")
	}
	blobName := ""
	if len(afterSplit) > 1 {
		blobName = name[len(afterSplit[0])+1:]
	}
	return afterSplit[0], blobName, nil
}

//AzureClient -> Delete function
// Delete specific Blob for input path
func (c *AzureClient) Remove(name string) error {
	afterSplit := strings.Split(name, "/")
	if len(afterSplit) == 1 && len(afterSplit[0]) == 32 {
		_, err := c.blobClient.DeleteContainerIfExists(name)
		if err != nil {
			return err
		}
		return nil
	}
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return err
	}
	_, err = c.blobClient.DeleteBlobIfExists(containerName, blobName)
	return err

}

// AzureClient -> Exist function
// support check the contianer or blob if exist or not
func (c *AzureClient) Exists(name string) (bool, error) {
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return false, err
	}
	if blobName != "" {
		return c.blobClient.BlobExists(containerName, blobName)
	} else {
		return c.blobClient.ContainerExists(containerName)
	}

}

// Azure prevent user renaming their blob
// Thus this function firstly copy the source blob,
// when finished, delete the source blob.
// http://stackoverflow.com/questions/3734672/azure-storage-blob-rename
func (c *AzureClient) moveBlob(dstContainerName, dstBlobName, srcContainerName, srcBlobName string, isContainerRename bool) error {
	dstBlobUrl := c.blobClient.GetBlobURL(dstContainerName, dstBlobName)
	srcBlobUrl := c.blobClient.GetBlobURL(srcContainerName, srcBlobName)
	if dstBlobUrl != srcBlobUrl {
		err := c.blobClient.CopyBlob(dstContainerName, dstBlobName, srcBlobUrl)
		if err != nil {
			return err
		}
		if !isContainerRename {
			err = c.blobClient.DeleteBlob(srcContainerName, srcBlobName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// AzureClient -> Rename function
// support rename contianer and blob
func (c *AzureClient) Rename(oldpath, newpath string) error {
	exist, err := c.Exists(oldpath)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("azureClient : oldpath does not exist")
	}
	srcContainerName, srcBlobName, err := convertToAzurePath(oldpath)
	if err != nil {
		return err
	}
	dstContainerName, dstBlobName, err := convertToAzurePath(newpath)
	if err != nil {
		return err
	}
	if srcBlobName == "" && dstBlobName == "" {
		resp, err := c.blobClient.ListBlobs(srcContainerName, storage.ListBlobsParameters{Marker: ""})
		if err != nil {
			return err
		}
		_, err = c.blobClient.CreateContainerIfNotExists(dstContainerName, storage.ContainerAccessTypeBlob)
		if err != nil {
			return err
		}

		for _, blob := range resp.Blobs {
			err = c.moveBlob(dstContainerName, blob.Name, srcContainerName, blob.Name, true)
			if err != nil {
				return err
			}
		}
		err = c.blobClient.DeleteContainer(srcContainerName)
		if err != nil {
			return err
		}
	} else if srcBlobName != "" && dstBlobName != "" {
		c.moveBlob(dstContainerName, dstBlobName, srcContainerName, srcBlobName, false)
	} else {
		return fmt.Errorf("Rename path does not match")
	}

	return nil
}

// AzureClient -> OpenReadCloser function
// implement by the providing function
func (c *AzureClient) OpenReadCloser(name string) (io.ReadCloser, error) {
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return nil, err
	}
	return c.blobClient.GetBlob(containerName, blobName)
}

//AzureClient -> OpenWriteCloser function
// If not exist, Create corresponding Container and blob.
func (c *AzureClient) OpenWriteCloser(name string) (io.WriteCloser, error) {
	exist, err := c.Exists(name)
	if err != nil {
		return nil, err
	}
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return nil, err
	}
	if !exist {
		_, err = c.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
		if err != nil {
			return nil, err
		}
		err = c.blobClient.CreateBlockBlob(containerName, blobName)
		if err != nil {
			return nil, err
		}
	}
	return &AzureFile{
		path:   name,
		logger: log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags),
		client: c.blobClient,
	}, nil
}

func (f *AzureFile) Write(b []byte) (int, error) {
	cnt, blob, err := convertToAzurePath(f.path)
	if err != nil {
		return 0, nil
	}
	blockList, err := f.client.GetBlockList(cnt, blob, storage.BlockListTypeAll)
	if err != nil {
		return 0, nil
	}

	blocksLen := len(blockList.CommittedBlocks) + len(blockList.UncommittedBlocks)
	var chunkSize int = storage.MaxBlobBlockSize
	inputSourceReader := bytes.NewReader(b)
	chunk := make([]byte, chunkSize)
	n, err := inputSourceReader.Read(chunk)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if err == io.EOF {
		return 0, fmt.Errorf("Need blob content")
	}
	for err != io.EOF {
		blockId := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", blocksLen-1)))
		data := chunk[:n]
		err = f.client.PutBlock(cnt, blob, blockId, data)
		if err != nil {
			return 0, err
		}
		blocksLen++
		n, err = inputSourceReader.Read(chunk)
		if err != nil && err != io.EOF {
			return 0, err
		}
	}

	blockList, err = f.client.GetBlockList(cnt, blob, storage.BlockListTypeAll)
	if err != nil {
		return 0, err
	}
	amendList := []storage.Block{}
	for _, v := range blockList.CommittedBlocks {
		amendList = append(amendList, storage.Block{v.Name, storage.BlockStatusCommitted})
	}
	for _, v := range blockList.UncommittedBlocks {
		amendList = append(amendList, storage.Block{v.Name, storage.BlockStatusUncommitted})
	}
	err = f.client.PutBlockList(cnt, blob, amendList)
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *AzureFile) Close() error {
	return nil
}

// AzureClient -> Glob function
// only supports '*', '?'
// Syntax:
// cntName?/part.*
func (c *AzureClient) Glob(pattern string) (matches []string, err error) {
	afterSplit := strings.Split(pattern, "/")
	cntPattern, blobPattern := afterSplit[0], afterSplit[1]
	if len(afterSplit) != 2 {
		return nil, fmt.Errorf("Glob pattern should follow the Syntax")
	}
	resp, err := c.blobClient.ListContainers(storage.ListContainersParameters{Prefix: ""})
	if err != nil {
		return nil, err
	}
	for _, cnt := range resp.Containers {
		matched, err := path.Match(cntPattern, cnt.Name)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		resp, err := c.blobClient.ListBlobs(cnt.Name, storage.ListBlobsParameters{Marker: ""})
		if err != nil {
			return nil, err
		}
		for _, v := range resp.Blobs {
			matched, err := path.Match(blobPattern, v.Name)
			if err != nil {
				return nil, err
			}
			if matched {
				matches = append(matches, cnt.Name+"/"+v.Name)
			}
		}
	}
	return matches, nil
}

// NewAzureClient function
// NewClient constructs a StorageClient and blobStorageClinet.
// This should be used if the caller wants to specify
// whether to use HTTPS, a specific REST API version or a
// custom storage endpoint than Azure Public Cloud.
// Recommended API version "2014-02-14"
// synax :
// AzurestorageAccountName, AzurestorageAccountKey, "core.chinacloudapi.cn", "2014-02-14", true
func NewAzureClient(accountName, accountKey, blobServiceBaseUrl, apiVersion string, useHttps bool) (*AzureClient, error) {
	cli, err := storage.NewClient(accountName, accountKey, blobServiceBaseUrl, apiVersion, useHttps)
	if err != nil {
		return nil, err
	}
	return &AzureClient{
		client:     &cli,
		blobClient: cli.GetBlobService(),
	}, nil
}
