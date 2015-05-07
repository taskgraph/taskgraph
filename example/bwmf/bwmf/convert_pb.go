package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/taskgraph/taskgraph/example/bwmf"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
)

func ConvertMatrixTxtToPb(confBytes []byte, path string) {

	config := &bwmf.Config{}
	unmarshErr := json.Unmarshal(confBytes, config)
	if unmarshErr != nil {
		panic(unmarshErr)
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)

	shard := &pb.MatrixShard{Row: make([]*pb.MatrixShard_RowData, 0)}
	var rowData *pb.MatrixShard_RowData

	// Read matrix elements from Stdio.
	// Each line represents an elem, formated as "rowID \t columnID \t count" without space.
	// The `rowID`s are guaranteed to be sorted and grouped (as in reducer tasks of a Hadoop job).
	curRow := -1
	for scanner.Scan() {
		line := scanner.Text()
		var (
			rowId    int
			columnId int32
			count    float32
		)

		n, sErr := fmt.Sscanf(line, "%d\t%d\t%f", &rowId, &columnId, &count)
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
		log.Panicf("Scanning lines error: ", sErr)
	}
	if curRow != -1 {
		shard.Row = append(shard.Row, rowData)
	}

	log.Println("num of rows: ", len(shard.Row))

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
			log.Panicf("Failed creating hdfs client %s", cltErr)
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
			log.Panicf("Failed creating azure client %s", cltErr)
		}
	default:
		log.Panicf("Unknow fs: %s", config.IOConf.Fs)
	}

	sErr = bwmf.SaveMatrixShard(client, shard, path)
	if sErr != nil {
		log.Panicf("Failed saving matrix shard to filesystem: %s", sErr)
	}
	log.Println("Saved a ", len(shard.Row), " rows matrix to ", path)
}
