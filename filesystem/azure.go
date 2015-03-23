package filesystem 
import ( 
    // "os_"
 	// "log"   
 	// "strings"
 	// "go/build"
    // "github.com/MSOpenTech/azure-sdk-for-go/management"
    // server "github.com/MSOpenTech/azure-sdk-for-go/management/core/http"
	// "bytes"
	// "io/ioutil"
	// "github.com/MSOpenTech/azure-sdk-for-go/storage"
	// "github.com/MSOpenTech/azure-sdk-for-go/core/http"
	// ""
	// "./azure-sdk-for-go/storage"
	// "bytes"
	// "io/ioutil"
	// "net/http"
	// "time"
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



// func (b BlobStorageClient) BlobExists(container, name string) (bool, error)

// func (c *storage.BlobStorageClient) Rename(oldpath, newpath string) error {
// 	c.Copy()
// }

// only supports '*', '?'
// Syntax:
//    /user/hdfs/etlpart.*
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

// func main() {
// 	// AzureClient

// 	// client, err := storage.NewClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==", "core.chinacloudapi.cn", "2014-02-14", true)
// 	// cli, err := newStorageServer()
// 	cli, err := NewAzureClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==", "core.chinacloudapi.cn", "2014-02-14", true)
	

// //Exist test	
// 	exist, err := cli.Exists("test1111111111111111111111111114/www2")
// 	fmt.Println(exist)
// 	if err != nil {
// 		// fmt.Println(err)
// 		fmt.Println(err)
// 	}

// 	// defer cli.DeleteContainer(cnt)
// 	// err = cli.blobClient.DeleteBlob("test1111111111111111111111111112", "www2.txt")


// //Put Block test
	
// 	blockId := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", 2)))
	
// 	blockId2 := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", 3)))
// 	chunk := make([]byte, storage.MaxBlobBlockSize)
// 	chunk = []byte("Hello")
// 	blockId1 := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", 1)))
// 	blockId3 := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%011d\n", 0)))
// 	cnt := "test1111111111111111111111111112"
// 	blob := "www2.txt"
// 	err = cli.blobClient.PutBlock(cnt, blob, blockId3, chunk)
// 	chunk = []byte("    !!!!!    ")
// 	err = cli.blobClient.PutBlock(cnt, blob, blockId1, chunk)

// 	wahaha := []storage.Block{}
// 	wahaha = append(wahaha, storage.Block{blockId1, storage.BlockStatusUncommitted})
// 	wahaha = append(wahaha, storage.Block{blockId3, storage.BlockStatusUncommitted})

// 	err = cli.blobClient.PutBlockList(cnt, blob, wahaha)
// 	if (err != nil) {
// 		fmt.Println(err)
// 	} 


// 	// body := []byte(randString(2024))
// 	// err = cli.blobClient.PutBlockBlob("test1111111111111111111111111112", "www2.txt", bytes.NewReader(body))

// 	// blobBody1, err := cli.blobClient.GetBlob("test1111111111111111111111111112", "www2.txt")
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }

// 	// b1, err := ioutil.ReadAll(blobBody1)
// 	// fmt.Println("!!!!")
// 	// fmt.Println(b1)

// 	testRead := strings.NewReader("wwwqqq")

// 	_, err = testRead.Read(chunk)
// 	fmt.Printf("%T", testRead)
// 	// fmt.Println("axiba! ", chunk)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	prop, err := cli.blobClient.GetBlobProperties("test1111111111111111111111111112", "www2.txt")
// 	fmt.Println(prop)
	
// 	chunk = []byte("ABCBurden")
	
// 	fmt.Printf("%011d\n", 1)
// 	fmt.Println(blockId, "2222")

// 	err = cli.blobClient.PutBlock("test1111111111111111111111111112", "www2.txt", blockId, chunk)
// 	chunk = []byte("ABCB")
// 	err = cli.blobClient.PutBlock("test1111111111111111111111111112", "www2.txt", blockId2, chunk)
// 	blockList, err := cli.blobClient.GetBlockList("test1111111111111111111111111112", "www2.txt", storage.BlockListTypeAll)
// 	// uncommitted, err := cli.blobClient.GetBlockList("test1111111111111111111111111112", "www2.txt", storage.BlockListTypeUncommitted)
// 	fmt.Println(blockList.CommittedBlocks)
// 	fmt.Println(blockList.UncommittedBlocks)
// 	fmt.Printf("%T\n", blockList)
// 	// blockList = append(blockList, storage.Block{blockId, storage.BlockStatusLatest})
// 	// var bbList []storage.Block{}
// 	bbList := []storage.Block{}
// 	for _, v := range blockList.UncommittedBlocks {
// 		fmt.Println(v.Name)
// 		bbList = append(bbList, storage.Block{v.Name, storage.BlockStatusUncommitted})
// 	}
// 	for _, v := range blockList.CommittedBlocks {
// 		fmt.Println(v.Name)
// 		bbList = append(bbList, storage.Block{v.Name, storage.BlockStatusCommitted})
// 		// bbList = append(bbList, storage.Block{v.Name, storage.BlockStatusCommitted})
// 		// bbList = append(bbList, storage.Block{v.Name, storage.BlockStatusCommitted})
// 	}
// 	// for _, v := range blockList.UncommittedBlocks {
// 	// 	fmt.Println(v.Name)
// 	// 	bbList = append(bbList, storage.Block{v.Name, storage.BlockStatusUncommitted})
// 	// }
	
	
// 	// bbList = append(bbList, storage.Block{blockId, storage.BlockStatusUncommitted})
// 	fmt.Println(bbList)
// 	//cli.blobClient.PutBlockList("test1111111111111111111111111112", "www2.txt", blockList)	

// 	err = cli.blobClient.PutBlockList("test1111111111111111111111111112", "www2.txt", bbList)
// 	if (err != nil) {
// 		fmt.Println(err)
// 	}	
// 	// defer cli.blobClient.Close()
// 	// defer cli.client.Close()
// 	blobBody, err := cli.blobClient.GetBlob("test1111111111111111111111111112", "www2.txt")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	// b, err := ioutil.ReadAll(blobBody)
// 	// fmt.Println("!!!!")
// 	// fmt.Println(b)

// 	blockList, err = cli.blobClient.GetBlockList("test1111111111111111111111111112", "www2.txt", storage.BlockListTypeAll)
// 	// uncommitted, err := cli.blobClient.GetBlockList("test1111111111111111111111111112", "www2.txt", storage.BlockListTypeUncommitted)
// 	fmt.Println(blockList.CommittedBlocks)
// 	fmt.Println(blockList.UncommittedBlocks)

// 	defer blobBody.Close()
// 	writeCloser, err := cli.OpenWriteCloser("tmptest0123456789012345678901234/testing")
// 	if err != nil {
// 		fmt.Printf("OpenWriteCloser failed: %v", err)
// 	}
// 	data := []byte("some data")

// 	fmt.Println(writeCloser)

// 	_, err = writeCloser.Write(data)
// 	if err != nil {
// 		fmt.Printf("Write failed: %v", err)
// 	}


// 	_, err = writeCloser.Write(data)
// 	if err != nil {
// 		fmt.Printf("Write failed: %v", err)
// 	}


// 	writeCloser.Close()
// 	readCloser, err := cli.OpenReadCloser("tmptest0123456789012345678901234/testing")
// 	if err != nil {
// 		fmt.Printf("OpenReadCloser failed: %v", err)
// 	}
// 	_, err = ioutil.ReadAll(readCloser)
// 	if err != nil {
// 		fmt.Printf("Read failed: %v", err)
// 	}
// 	readCloser.Close()

// 	// if bytes.Compare(b, data) != 0 {
// 	// 	fmt.Printf("Read result isn't correct. Get = %s, Want = %s", string(b), string(data))
// 	// }



// 	// c := client.(*HdfsClient).client
// 	// c.Remove("/tmp/testing")


// 	// blob := randString(20)
// 	// size := int64(10 * 1024 * 1024) // larger than we'll use
// 	// if err := cli.PutPageBlob(cnt, blob, size); err != nil {
// 	// 	log.Fatal(err)
// 	// }

// 	// chunk1 := []byte(randString(1024))
// 	// chunk2 := []byte(randString(512))
// 	// // Append chunks
// 	// if err := cli.PutPage(cnt, blob, 0, int64(len(chunk1)-1), PageWriteTypeUpdate, chunk1); err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// if err := cli.PutPage(cnt, blob, int64(len(chunk1)), int64(len(chunk1)+len(chunk2)-1), PageWriteTypeUpdate, chunk2); err != nil {
// 	// 	log.Fatal(err)
// 	// }

// 	// // Verify contents
// 	// out, err := cli.GetBlobRange(cnt, blob, fmt.Sprintf("%v-%v", 0, len(chunk1)+len(chunk2)))
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// blobContents, err := ioutil.ReadAll(out)
// 	// defer out.Close()
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// if expected := append(chunk1, chunk2...); reflect.DeepEqual(blobContents, expected) {
// 	// 	log.Fatalf("Got wrong blob.\nGot:%d bytes, Expected:%d bytes", len(blobContents), len(expected))
// 	// }
// 	// out.Close()

// 	// // Overwrite first half of chunk1
// 	// chunk0 := []byte(randString(512))
// 	// if err := cli.PutPage(cnt, blob, 0, int64(len(chunk0)-1), PageWriteTypeUpdate, chunk0); err != nil {
// 	// 	log.Fatal(err)
// 	// }

// 	// // Verify contents
// 	// out, err = cli.GetBlobRange(cnt, blob, fmt.Sprintf("%v-%v", 0, len(chunk1)+len(chunk2)))
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// blobContents, err = ioutil.ReadAll(out)
// 	// defer out.Close()
// 	// if err != nil {
// 	// 	log.Fatal(err)
// 	// }
// 	// if expected := append(append(chunk0, chunk1[512:]...), chunk2...); reflect.DeepEqual(blobContents, expected) {
// 	// 	log.Fatalf("Got wrong blob.\nGot:%d bytes, Expected:%d bytes", len(blobContents), len(expected))
// 	// }

// //List all files

// 	fmt.Println(exist)
// 	resp, err := cli.blobClient.ListContainers(storage.ListContainersParameters{Prefix: ""})
// 	if err != nil {
// 		fmt.Println(err)	
// 	}

// 	for _, c := range resp.Containers {
// 		fmt.Println(c.Name)
// 		resp, err := cli.blobClient.ListBlobs(c.Name, storage.ListBlobsParameters{
// 			Marker:     ""})
// 		if err != nil {
// 			fmt.Println(err)	
// 		}

// 		for _, v := range resp.Blobs {
// 			fmt.Println("-----", v.Name)
// 		}
		
// 	}
// 	globPath := "test*/.*"
// 	names, err := cli.Glob(globPath)
// 	if err != nil {
// 		fmt.Printf("Glob(%s) failed: %v", globPath, err)
// 	}
// 	fmt.Println(names)
// 	// connectStorageSever()

// }