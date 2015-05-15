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
	"github.com/taskgraph/taskgraph/filesystem"
)

func main() {

	resultPath := flag.String("output_path", "", "Path to save the persisted matrix data.")
	configFile := flag.String("task_config", "", "Path to task config json file.")

	flag.Parse()

	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed loading config from %s with error %s.", *configFile, err)
	}

	// reading mat from stdin
	shard, err := txtToMatPb()
	if err != nil {
		log.Fatalf("Failed converting txt data to mat pb. Error %s.", err)
	}

	err = saveResult(shard, config, *resultPath)
	if err != nil {
		log.Fatalf("Failed persisting matrix pb data into %s with error %s.", *resultPath, err)
	}
}

func loadConfig(path string) (*bwmf.Config, error) {
	crd, oErr := filesystem.NewLocalFSClient().OpenReadCloser(path)
	if oErr != nil {
		return nil, oErr
	}
	confBytes, rdErr := ioutil.ReadAll(crd)
	if rdErr != nil {
		return nil, rdErr
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

		n, sErr := fmt.Sscanf(line, "%d%d%f", &rowId, &columnId, &count)
		if sErr != nil || n != 3 {
			log.Printf("Failed parse line %s with error %s.", line, sErr)
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
	sErr := scanner.Err()
	if sErr != nil {
		return nil, sErr
	}
	if curRow != -1 {
		shard.Row = append(shard.Row, rowData)
	}
	log.Println("num of rows: ", len(shard.Row))
	return shard, nil
}

func saveResult(shard *pb.MatrixShard, config *bwmf.Config, path string) error {
	// saving via filesystem
	var client filesystem.Client
	var cltErr error
	switch config.IOConf.Fs {
	case "local":
		client = filesystem.NewLocalFSClient()
	case "hdfs":
		client, cltErr = filesystem.NewHdfsClient(
			config.IOConf.HdfsConf.NamenodeAddr,
			config.IOConf.HdfsConf.WebHdfsAddr,
			config.IOConf.HdfsConf.User,
		)
		if cltErr != nil {
			return fmt.Errorf("Failed creating hdfs client %s", cltErr)
		}
	case "azure":
		client, cltErr = filesystem.NewAzureClient(
			config.IOConf.AzureConf.AccountName,
			config.IOConf.AzureConf.AccountKey,
			config.IOConf.AzureConf.BlogServiceBaseUrl,
			config.IOConf.AzureConf.ApiVersion,
			config.IOConf.AzureConf.UseHttps,
		)
		if cltErr != nil {
			return fmt.Errorf("Failed creating azure client %s", cltErr)
		}
	default:
		return fmt.Errorf("Unknow fs: %s", config.IOConf.Fs)
	}

	sErr := bwmf.SaveMatrixShard(client, shard, path)
	if sErr != nil {
		return fmt.Errorf("Failed saving matrix shard to filesystem: %s", sErr)
	}
	log.Println("Saved a ", len(shard.Row), " rows matrix to ", path)
	return nil
}
