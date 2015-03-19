package main 
import ( 
    // "os_"
 	// "log"   
 	// "strings"
 	// "go/build"
    // "github.com/MSOpenTech/azure-sdk-for-go/management"
    // server "github.com/MSOpenTech/azure-sdk-for-go/management/core/http"
	"github.com/MSOpenTech/azure-sdk-for-go/storage"
	// "github.com/MSOpenTech/azure-sdk-for-go/core/http"
	// ""
	// "./azure-sdk-for-go/storage"
	"fmt"
	"strings"
	// "bytes"
	"crypto/rand"
	// "io/ioutil"
	// "net/http"
	// "time"
)


// type azureConfig struct {
// 	accountName    string
// 	accountKey        string
// 	baseUrl string
// }

type AzureClient struct {
	client *storage.StorageClient
	blobClient *storage.BlobStorageClient
}

func convertToAzurePath(name string) (string, string, error) {
	afterSplit := strings.Split(name, "/")
	if len(afterSplit) != 2 || len(afterSplit[0]) != 32 {
		return "","",fmt.Errorf("Azure : Need Correct Path Name")
	}
	return afterSplit[0], afterSplit[1], nil
} 

// func (c *Glob) OpenReadCloser(name string) (io.ReadCloser, error) {
// 	return c.client.Open(name)
// }

// func (c *storage.BlobStorageClient) OpenWriteCloser(name string) (io.WriteCloser, error) {
// 	exist, err := c.Exists(name)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if !exist {
// 		err := c.client.CreateEmptyFile(name)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	return &HdfsFile{
// 		path:       name,
// 		logger:     log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags),
// 		hdfsConfig: c.hdfsConfig,
// 	}, nil
// }

func (c *AzureClient) Exists(name string) (bool, error) {
	containerName, blobName, err := convertToAzurePath(name)
	if err != nil {
		return false, err
	}
	return c.blobClient.BlobExists(containerName, blobName)
	// _, err := c.client.Stat(name)
	// return existCommon(err)
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

// func NewHdfsClient(namenodeAddr, webHdfsAddr, user string) (Client, error) {
// 	client, err := hdfs.NewForUser(namenodeAddr, user)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &HdfsClient{
// 		client: client,
// 		hdfsConfig: hdfsConfig{
// 			namenodeAddr: namenodeAddr,
// 			webHdfsAddr:  webHdfsAddr,
// 			user:         user,
// 		},
// 	}, nil
// }


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
	// if err != nil {
	// 	log.Fatal(err)
	// }
	


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
	exist, err := cli.Exists("11111111111111111111111111111111/sa")
	if (err != nil) {
		fmt.Println(err)
	} 
	fmt.Println(exist)

	// connectStorageSever()

}