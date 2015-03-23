package filesystem 
import (
	"io"
	"os"
	"log"
	"github.com/MSOpenTech/azure-sdk-for-go/storage"
	"fmt"
	"strings"
	"regexp"
	"crypto/rand"
	"encoding/base64"
)

type AzureClient struct {
	client *storage.StorageClient
	blobClient *storage.BlobStorageClient
}

type AzureFile struct {
	path   string
	logger *log.Logger
	client *storage.BlobStorageClient
}

// convertToAzurePath function
// like this pattern "ContainerName/BlobName"
// Due to Azure restriction, the length of ContainerName must be 32
func convertToAzurePath(name string) (string, string, error) {
	afterSplit := strings.Split(name, "/")
	if len(afterSplit) != 2 || len(afterSplit[0]) != 32 {
		return "", "", fmt.Errorf("AzureClient : Need Correct Path Name")
	}
	return afterSplit[0], afterSplit[1], nil
} 

// AzureClient -> Exist function 
// Only check the BlobName if exist or not
// User should Provide corresponding ContainerName
func (c *AzureClient) Exists(name string) (bool, error) {
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return false, err
	}
	return  c.blobClient.BlobExists(containerName, blobName)
}

// AzureClient -> Rename function 
// Azure prevent user renaming their blob
// Thus this function firstly copy the source blob, 
// when finished, delete the source blob.
// http://stackoverflow.com/questions/3734672/azure-storage-blob-rename
func (c *AzureClient) Rename(oldpath, newpath string) error {
	exist, err := c.Exists(oldpath)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("AzureClient : oldpath doesnot exist")
	}
	srcContainerName, srcBlobName, err := convertToAzurePath(oldpath)
	if err != nil {
		return err
	}
	dstContainerName, dstBlobName, err := convertToAzurePath(newpath)
	if err != nil {
		return err
	}
	dstBlobUrl := c.blobClient.GetBlobUrl(dstContainerName, dstBlobName)
	srcBlobUrl := c.blobClient.GetBlobUrl(srcContainerName, srcBlobName)
	c.blobClient.CopyBlob(dstContainerName, dstBlobName, srcBlobUrl)
	if dstBlobUrl != srcBlobUrl {
		fmt.Println(srcContainerName, srcBlobName, dstContainerName, dstBlobName, srcBlobUrl)
		err = c.blobClient.DeleteBlob(srcContainerName, srcBlobName)
		if err != nil {
			return err
		}
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
// At present, AzureFile.Write has a capacity restriction(10 * 1024 * 1024 bytes). 
// I will implent unlimited version in the future.
func (c *AzureClient) OpenWriteCloser(name string) (io.WriteCloser, error) {
	exist, err := c.Exists(name)
	if err != nil {
		return nil, err
	}

	fmt.Println("!!!")

	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return nil, err
	}

	if !exist {
		_, err := c.blobClient.CreateContainerIfNotExists(containerName, storage.ContainerAccessTypeBlob)
		if err != nil {
			return nil, err
		}
		err = c.blobClient.CreateBlockBlob(containerName, blobName)
		if err != nil {
			return nil, err
		}
	}

	

	return &AzureFile{
		path  : name,
		logger : log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags),
		client : c.blobClient,
	}, nil

	return nil, nil
}



func (f *AzureFile) Write(b []byte) (int, error) {
	cnt, blob, err := convertToAzurePath(f.path)
	if err != nil {
		return 0, nil
	}
	blockList, err := f.client.GetBlockList(cnt, blob, storage.BlockListTypeAll)
	fmt.Println(len(blockList.CommittedBlocks))
	fmt.Println(len(blockList.UncommittedBlocks))

	blocksLen := len(blockList.CommittedBlocks) + len(blockList.UncommittedBlocks)

	blockId := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", blocksLen - 1)))

	err = f.client.PutBlock(cnt, blob, blockId, b)
	
	blockList, err = f.client.GetBlockList(cnt, blob, storage.BlockListTypeAll)


	amendList := []storage.Block{}
	for _, v := range blockList.CommittedBlocks {
		fmt.Println(v.Name)
		amendList = append(amendList, storage.Block{v.Name, storage.BlockStatusCommitted})	
	}
	
	for _, v := range blockList.UncommittedBlocks {
		fmt.Println(v.Name)
		amendList = append(amendList, storage.Block{v.Name, storage.BlockStatusUncommitted})
	}
	
	err = f.client.PutBlockList(cnt, blob, amendList)
	if (err != nil) {
		fmt.Println(err)	
	}
	return 0, nil
}

func (f *AzureFile) Close() error {
	return nil
}

// AzureClient -> Glob function
// Syntax : Adf{1,3}?ee*/ytsd.*
// Follow regexp syntax except "/"
func (c *AzureClient) Glob(pattern string) (matches []string, err error) {
	afterSplit := strings.Split(pattern, "/")
	cntPattern, blobPattern := afterSplit[0], afterSplit[1]
	if len(afterSplit) != 2 {	
		return nil, fmt.Errorf("Glob pattern should follow the Syntax")
	}
	fmt.Println(cntPattern, " ", blobPattern)
	resp, err := c.blobClient.ListContainers(storage.ListContainersParameters{Prefix: ""})
	if err != nil {
		fmt.Println(err)	
	}

	for _, cnt := range resp.Containers {
		if match, err := regexp.MatchString(cntPattern, cnt.Name); match && err == nil {
			fmt.Println("in Containers ", cnt.Name)
			resp, err := c.blobClient.ListBlobs(cnt.Name, storage.ListBlobsParameters{
				Marker:     ""})
			if err != nil {
				fmt.Println(err)	
			}

			for _, v := range resp.Blobs {
				if match, err := regexp.MatchString(blobPattern, v.Name); match && err == nil {
					matches = append(matches, cnt.Name + "/" + v.Name)
				}
			}
		}
		
	}
	return matches, nil
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

func NewAzureClient(accountName, accountKey, blobServiceBaseUrl, apiVersion string, useHttps bool) (*AzureClient, error) {
	cli, err := storage.NewClient(accountName, accountKey, blobServiceBaseUrl, apiVersion, useHttps)
	if err != nil {
		return nil, err
	}
	return &AzureClient{
		client : &cli,
		blobClient : cli.GetBlobService(),
	}, nil
}
