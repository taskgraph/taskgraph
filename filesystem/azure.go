package main 
import ( 
    // "os_"
 	"log"   
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

func getStorageServer() (*storage.BlobStorageClient) {
	client, err := storage.NewClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==", "core.chinacloudapi.cn", "2014-02-14", true)
	if err != nil {
		log.Fatal(err)
	}
	cli := client.GetBlobService()
	return cli
}


// func (c storage.StorageClient) GetBlobService() *storage.BlobStorageClient {
// 	return &storage.BlobStorageClient{c}
// }


func connectStorageSever() {
	// client, err := storage.NewBasicClient("spluto", "b7yy+C33a//uLE62Og9CkKDHRLNErMrbX40nKUxiTimgOvkP3MhEbjObmRxumda9grCwY8zqL6nLNcKCAS40Iw==")
	
	
	// fmt.Println(type(client))
	// fmt.Println(client.accountName)
	cli := getStorageServer()
	cnt := randString(32)
	blob := randString(20)
	// body := []byte(randString(100))
	// expiry := time.Now().UTC().Add(time.Hour)
	// permissions := "r"

	// err = cli.CreateContainer(cnt, storage.ContainerAccessTypePrivate)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	err := cli.CreateContainer(cnt, storage.ContainerAccessTypeBlob)
	if err != nil {
		// fmt.Println(err)
		log.Fatal(err)
	}
	fmt.Println("!!!!df")
	// defer cli.DeleteContainer(cnt)
	err = cli.PutBlockBlob(cnt, blob, strings.NewReader("Hello!"))
	if err != nil {
		log.Fatal(err)
	}
	defer cli.DeleteBlob(cnt, blob)

	ok, err := cli.BlobExists(cnt, blob+".foo")
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		log.Fatal("Non-existing blob returned as existing: %s/%s", cnt, blob)
	}

	ok, err = cli.BlobExists(cnt, blob)
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		log.Fatal("Existing blob returned as non-existing: %s/%s", cnt, blob)
	}

	// defer cli.DeleteContainer(cnt)
	// log.Printf("3333")
	// err = cli.PutBlockBlob(cnt, blob, bytes.NewReader(body))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// sasUri, err := cli.GetBlobSASURI(cnt, blob, expiry, permissions)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// resp, err := http.Get(sasUri)
	// if err != nil {
	// 	// t.Logf("SAS URI: %s", sasUri)
	// 	log.Fatal(err)
	// }

	// blobResp, err := ioutil.ReadAll(resp.Body)
	// defer resp.Body.Close()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// if resp.StatusCode != http.StatusOK {
	// 	log.Fatalf("Non-ok status code: %s", resp.Status)
	// }

	// if len(blobResp) != len(body) {
	// 	log.Fatalf("Wrong blob size on SAS URI. Expected: %d, Got: %d", len(body), len(blobResp))
	// }
}

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
	
	connectStorageSever()

}