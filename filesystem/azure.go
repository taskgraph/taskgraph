package main 
import ( 
    // "os_"
 	// "log"   
 	// "strings"
 	// "go/build"
    // "github.com/MSOpenTech/azure-sdk-for-go/management"
    // server "github.com/MSOpenTech/azure-sdk-for-go/management/core/http"
	"io"
	"log"
	"github.com/MSOpenTech/azure-sdk-for-go/storage"
	// "github.com/MSOpenTech/azure-sdk-for-go/core/http"
	// ""
	// "./azure-sdk-for-go/storage"
	"fmt"
	"strings"
	// "bytes"
	"crypto/rand"
	// "bytes"
	// "io/ioutil"
	// "net/http"
	// "time"
)


// type azureConfig struct {
// 	accountName    string
// 	accountKey        string
// 	baseUrl string
// }

type azureConfig struct {
	accountName string
	accountKey  []byte
	useHttps    bool
	baseUrl     string
	apiVersion  string
}

type AzureClient struct {
	client *storage.StorageClient
	blobClient *storage.BlobStorageClient
}

type AzureFile struct {
	path   string
	logger *log.Logger
	azureConfig
	// buffer
}


/*
	convertToAzurePath function
	----------------------------------
	like this pattern "ContainerName/BlobName"
	Due to Azure restriction, the length of ContainerName must be 32
*/


func convertToAzurePath(name string) (string, string, error) {
	afterSplit := strings.Split(name, "/")
	if len(afterSplit) != 2 || len(afterSplit[0]) != 32 {
		return "", "", fmt.Errorf("AzureClient : Need Correct Path Name")
	}
	return afterSplit[0], afterSplit[1], nil
} 

/*
	AzureClient -> Exist function 
	-----------------------------------------
	Only check the BlobName if exist or not
	User should Provide corresponding ContainerName
	PS : Need check ContainerName ?
*/

func (c *AzureClient) Exists(name string) (bool, error) {
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return false, err
	}
	// exist, err := c.blobClient.ContainerExists(containerName)
	// if err != nil {
	// 	return false, err
	// }
	// if exist {
	// 	fmt.Println(containerName, blobName, exist)
	return  c.blobClient.BlobExists(containerName, blobName)
	// }
	// return exist, err
	

}

/*
	AzureClient -> Rename function 
	----------------------------------------
	Azure prevent user renaming their blob
	Thus this function firstly copy the source blob, 
	when finished, delete the source blob.
	http://stackoverflow.com/questions/3734672/azure-storage-blob-rename
*/

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

	// _, err = c.blobClient.CreateContainerIfNotExists(dstContainerName, storage.ContainerAccessTypePrivate)
	// if err != nil {
	// 	return err
	// }
	

	// err = c.blobClient.PutBlockBlob(dstContainerName, dstBlobName, bytes.NewReader(body))
	// if err != nil {
	// 	return err
	// }
	dstBlobUrl := c.blobClient.GetBlobUrl(dstContainerName, dstBlobName)
	srcBlobUrl := c.blobClient.GetBlobUrl(srcContainerName, srcBlobName)
	// fmt.Println(srcContainerName, srcBlobName, dstContainerName, dstBlobName, srcBlobUrl)
	// exist, err := c.blobClient.CreateContainerIfNotExists(dstContainerName, ContainerAccessTypeContainer)
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

/*
	AzureClient -> OpenReadCloser function
	-----------------------------------------
*/

func (c *AzureClient) OpenReadCloser(name string) (io.ReadCloser, error) {
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return nil, err
	}
	return c.blobClient.GetBlob(containerName, blobName)
}


/*
	AzureClient -> OpenWriteCloser function
	-----------------------------------------
*/


func (c *AzureClient) OpenWriteCloser(name string) (io.WriteCloser, error) {
	return nil, nil
}

func (f *AzureFile) Write(b []byte) (int, error) {
	return 0, nil
}

func (c *AzureClient) Glob(pattern string) (matches []string, err error) {
	return nil, nil
}

// func (b BlobStorageClient) BlobExists(container, name string) (bool, error)

// func (c *storage.BlobStorageClient) Rename(oldpath, newpath string) error {
// 	c.Copy()
// }

// only supports '*', '?'
// Syntax:
//    /user/hdfs/etl*/part.*
// func (c *storage.BlobStorageClient) Glob(pattern string) (matches []string, err error) {
// 	if pattern == "" {
// 		return nil, fmt.Errorf("Glob pattern shouldn't be empty")
// 	}
// 	if pattern[len(pattern)-1] == '/' {
// 		return nil, fmt.Errorf("Glob pattern shouldn't be a directory")
// 	}
// 	// names will have all the pathnames of the pattern.
// 	// e.g. "/a/b/c" => [a, b, c]
// 	var names []string
// 	for path.Dir(pattern) != "/" {
// 		names = append(names, path.Base(pattern))
// 		pattern = path.Dir(pattern)
// 	}
// 	names = append(names, pattern[1:len(pattern)])
// 	for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
// 		names[i], names[j] = names[j], names[i]
// 	}
// 	return c.glob("/", names)
// }

// func (c *storage.BlobStorageClient) glob(dir string, names []string) (m []string, err error) {
// 	name := names[0]
// 	var dirs []string
// 	if hasMeta(name) {
// 		fileInfos, err := c.client.ReadDir(dir)
// 		if err != nil {
// 			return nil, err
// 		}
// 		for _, fi := range fileInfos {
// 			matched, err := path.Match(name, fi.Name())
// 			if err != nil {
// 				return nil, err
// 			}
// 			if matched {
// 				dirs = append(dirs, path.Join(dir, fi.Name()))
// 			}
// 		}
// 	} else {
// 		dirs = append(dirs, path.Join(dir, name))
// 	}
// 	for _, pathname := range dirs {
// 		if len(names) == 1 {
// 			exist, err := c.Exists(pathname)
// 			if err != nil {
// 				return nil, err
// 			}
// 			if exist {
// 				m = append(m, pathname)
// 			}
// 		} else {
// 			return c.glob(pathname, names[1:len(names)])

// 		}
// 	}
// 	return
// }


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


// func (c storage.StorageClient) GetBlobService() *storage.BlobStorageClient {
// 	return &storage.BlobStorageClient{c}
// }


// func connectStorageSever() {
// 	// client, err := storage.NewBasicClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==")
	
	
// 	// fmt.Println(type(client))
// 	// fmt.Println(client.accountName)
// 	cli, err := newStorageServer()
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	cnt := randString(32)
// 	blob := randString(20)
// 	// body := []byte(randString(100))
// 	// expiry := time.Now().UTC().Add(time.Hour)
// 	// permissions := "r"

// 	// err = cli.CreateContainer(cnt, storage.ContainerAccessTypePrivate)
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	err = cli.CreateContainer(cnt, storage.ContainerAccessTypeBlob)
// 	if err != nil {
// 		// fmt.Println(err)
// 		log.Fatal(err)
// 	}
// 	fmt.Println("!!!!df")
// 	// defer cli.DeleteContainer(cnt)
// 	err = cli.PutBlockBlob(cnt, blob, strings.NewReader("Hello!"))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer cli.DeleteBlob(cnt, blob)

// 	ok, err := cli.BlobExists(cnt, blob+".foo")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	if ok {
// 		log.Fatal("Non-existing blob returned as existing: %s/%s", cnt, blob)
// 	}

// 	ok, err = cli.BlobExists(cnt, blob)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	if !ok {
// 		log.Fatal("Existing blob returned as non-existing: %s/%s", cnt, blob)
// 	}

// 	// defer cli.DeleteContainer(cnt)
// 	// log.Printf("3333")
// 	// err = cli.PutBlockBlob(cnt, blob, bytes.NewReader(body))
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// sasUri, err := cli.GetBlobSASURI(cnt, blob, expiry, permissions)
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }

// 	// resp, err := http.Get(sasUri)
// 	// if err != nil {
// 	// 	// t.Logf("SAS URI: %s", sasUri)
// 	// 	log.Fatal(err)
// 	// }

// 	// blobResp, err := ioutil.ReadAll(resp.Body)
// 	// defer resp.Body.Close()
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// if resp.StatusCode != http.StatusOK {
// 	// 	log.Fatalf("Non-ok status code: %s", resp.Status)
// 	// }

// 	// if len(blobResp) != len(body) {
// 	// 	log.Fatalf("Wrong blob size on SAS URI. Expected: %d, Got: %d", len(body), len(blobResp))
// 	// }
// }

// cli, err := getBlobClient()
// 	if err != nil {
// 		log.Fatal(err)
// 	}
	


// func getBlobClient() (*BlobStorageClient, error) {
// 	name := os.Getenv("ACCOUNT_NAME")
// 	if name == "" {
// 		return nil, errors.New("ACCOUNT_NAME not set, need an empty storage account to test")
// 	}
// 	key := os.Getenv("ACCOUNT_KEY")
// 	if key == "" {
// 		return nil, errors.New("ACCOUNT_KEY not set")
// 	}
// 	cli, err := NewBasicClient(name, key)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return cli.GetBlobService(), nil
// }

func main() {
	// AzureClient

	// client, err := storage.NewClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==", "core.chinacloudapi.cn", "2014-02-14", true)
	// cli, err := newStorageServer()
	cli, err := NewAzureClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==", "core.chinacloudapi.cn", "2014-02-14", true)
	exist, err := cli.Exists("test1111111111111111111111111114/www2")
	err = cli.Rename("test1111111111111111111111111112/www2.txt", "test1111111111111111111111111112/b.txt")
	// err = cli.blobClient.CreateContainer("test1111111111111111111111111114", storage.ContainerAccessTypeBlob)
	// if err != nil {
	// 	// fmt.Println(err)
	// 	log.Fatal(err)
	// }
	// defer cli.DeleteContainer(cnt)
	// err = cli.blobClient.DeleteBlob("test1111111111111111111111111112", "www2.txt")
	err = cli.blobClient.PutBlockBlob("test1111111111111111111111111112", "www2.txt", strings.NewReader("Hello!"))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	if (err != nil) {
		fmt.Println(err)
	} 


	fmt.Println(exist)
	resp, err := cli.blobClient.ListContainers(storage.ListContainersParameters{Prefix: ""})
	if err != nil {
		fmt.Println(err)	
	}

	for _, c := range resp.Containers {
		fmt.Println(c.Name)
		resp, err := cli.blobClient.ListBlobs(c.Name, storage.ListBlobsParameters{
			Marker:     ""})
		if err != nil {
			fmt.Println(err)	
		}

		for _, v := range resp.Blobs {
			fmt.Println("-----", v.Name)
		}
		
	}
	// connectStorageSever()

}