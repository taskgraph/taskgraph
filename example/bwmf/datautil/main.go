package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/taskgraph/taskgraph/example/bwmf"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	fs "github.com/taskgraph/taskgraph/filesystem"
)

func main() {

	pbFilePath := flag.String("file_path", "", "Path to save the persisted matrix data.")
	configFile := flag.String("task_config", "", "Path to task config json file.")
	convertType := flag.String("convert", "txt2pb", "txt2pb(default): Convert stdin txt representation of the matrix into a pb file; pb2txt: convert a pb file a txt and output via Stdout.")

	flag.Parse()

	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed loading config from %s with error %s.", *configFile, err)
	}

	switch *convertType {
	case "txt2pb":
		// reading mat from stdin
		shard, err := txtToMatPb()
		if err != nil {
			log.Fatalf("Failed converting txt data to mat pb. Error %s.", err)
		}
		err = saveResult(shard, config, *pbFilePath)
		if err != nil {
			log.Fatalf("Failed persisting matrix pb data into %s with error %s.", *pbFilePath, err)
		}
	case "pb2txt":
		shard, err := loadPbBuffer(config, *pbFilePath)
		if err != nil {
			log.Fatalf("Failed loading matrix pb data from %s with error %s.", *pbFilePath, err)
		}
		// parsing shard and output rows to stdout
		err = matPbToTxt(shard)
		if err != nil {
			log.Fatalf("Failed printing matrix data with error %s.", err)
		}
	default:
		log.Fatal("Please choose a convertion type via -convert: (txt2pb) or (pb2txt).")
	}

}

func loadConfig(path string) (*bwmf.Config, error) {
	crd, err := fs.NewLocalFSClient().OpenReadCloser(path)
	if err != nil {
		return nil, err
	}
	confBytes, err := ioutil.ReadAll(crd)
	if err != nil {
		return nil, err
	}

	config := &bwmf.Config{}
	unmarshErr := json.Unmarshal(confBytes, config)
	if unmarshErr != nil {
		return nil, unmarshErr
	}
	return config, nil
}

func txtToMatPb() (*pb.MatrixShard, error) {
	// Each line represents an elem, formated as "rowID columnID count".
	// The `rowID`s are guaranteed to be sorted and grouped (as in reducer tasks of a Hadoop job).
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)

	shard := &pb.MatrixShard{Row: make([]*pb.MatrixShard_RowData, 0)}
	var rowData *pb.MatrixShard_RowData
	curRow := -1
	for scanner.Scan() {
		line := scanner.Text()
		var (
			rowId    int
			columnId int32
			count    float32
		)

		n, err := fmt.Sscanf(line, "%d%d%f", &rowId, &columnId, &count)
		if err != nil || n != 3 {
			log.Printf("Failed parse line %s with error %s.", line, err)
			continue
		}
		if curRow != rowId {
			// a new row
			if curRow != -1 {
				shard.Row = append(shard.Row, rowData)
			}
			curRow = rowId
			rowData = &pb.MatrixShard_RowData{
				RowId: int32(curRow),
				At:    make(map[int32]float32),
			}
		}
		rowData.At[columnId] = count
	}
	err := scanner.Err()
	if err != nil {
		return nil, err
	}
	if curRow != -1 {
		shard.Row = append(shard.Row, rowData)
	}
	log.Println("num of rows: ", len(shard.Row))
	return shard, nil
}

func matPbToTxt(shard *pb.MatrixShard) error {
	for _, row := range shard.Row {
		fmt.Printf("%d\t", row.RowId)
		for col, val := range row.At {
			fmt.Printf("%d %f ", col, val)
		}
		fmt.Printf("\n")
	}
	return nil
}

func getFsClient(config *bwmf.Config) (fs.Client, error) {
	var client fs.Client
	var cltErr error
	switch config.IOConf.Fs {
	case "local":
		client = fs.NewLocalFSClient()
	case "hdfs":
		client, cltErr = fs.NewHdfsClient(
			config.IOConf.HdfsConf.NamenodeAddr,
			config.IOConf.HdfsConf.WebHdfsAddr,
			config.IOConf.HdfsConf.User,
		)
		if cltErr != nil {
			return nil, fmt.Errorf("Failed creating hdfs client %s", cltErr)
		}
	case "azure":
		client, cltErr = fs.NewAzureClient(
			config.IOConf.AzureConf.AccountName,
			config.IOConf.AzureConf.AccountKey,
			config.IOConf.AzureConf.BlogServiceBaseUrl,
			config.IOConf.AzureConf.ApiVersion,
			config.IOConf.AzureConf.UseHttps,
		)
		if cltErr != nil {
			return nil, fmt.Errorf("Failed creating azure client %s", cltErr)
		}
	default:
		return nil, fmt.Errorf("Unknow fs: %s", config.IOConf.Fs)
	}
	return client, nil
}

func saveResult(shard *pb.MatrixShard, config *bwmf.Config, path string) error {
	client, err := getFsClient(config)
	if err != nil {
		return fmt.Errorf("Failed getting filesystem.Client: %s", err)
	}
	return bwmf.SaveMatrixShard(client, shard, path)
}

func loadPbBuffer(config *bwmf.Config, path string) (*pb.MatrixShard, error) {
	client, err := getFsClient(config)
	if err != nil {
		return nil, fmt.Errorf("Failed getting filesystem.Client: %s", err)
	}
	return bwmf.LoadMatrixShard(client, path)
}
