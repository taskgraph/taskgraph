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
	dimN := flag.Int("n", 100, "Num of columns of the matrix. Required when convert=txt2pb.")

	flag.Parse()

	config, err := loadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed loading config from %s with error %s.", *configFile, err)
	}

	switch *convertType {
	case "txt2pb":
		// reading mat from stdin
		shard, err := txtToMatPb(*dimN)
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

// NOTE(baigang): Each line represents an elem, formated as "rowID columnID count".
// The `rowID`s are ASSUMED to be sorted and grouped (as in reducer tasks of a Hadoop job).
func txtToMatPb(n int) (*pb.MatrixShard, error) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)

	val := make([]float32, 0)
	ir := make([]uint32, 0)
	jc := make([]uint32, 0)
	ir = append(ir, 0)
	var (
		rowId    int
		columnId uint32
		value    float32
	)
	curRow := -1
	for scanner.Scan() {
		line := scanner.Text()
		n, err := fmt.Sscanf(line, "%d%d%f", &rowId, &columnId, &value)
		if err != nil || n != 3 {
			log.Printf("Failed parse line %s with error %s.", line, err)
			continue
		}
		if curRow != rowId {
			// a new row
			if curRow != -1 {
				ir = append(ir, uint32(len(val)))
			}
			curRow = rowId
		}
		val = append(val, value)
		jc = append(jc, columnId)
	}
	err := scanner.Err()
	if err != nil {
		return nil, err
	}
	if curRow != -1 {
		ir = append(ir, uint32(len(val)))
	}
	log.Println("num of rows: ", len(ir)-1)
	log.Println("num of elems: ", len(val))

	return &pb.MatrixShard{
		IsSparse: false,
		M:        uint32(len(ir) - 1),
		N:        uint32(n),
		Val:      val,
		Ir:       ir,
		Jc:       jc,
	}, nil
}

func matPbToTxt(shard *pb.MatrixShard) error {
	if len(shard.Val) != len(shard.Jc) || len(shard.Ir) != int(shard.M+1) {
		return fmt.Errorf("MatrixShard data corrupted. Len val: %d, len Jc: %d, len Ia: %d, shard M: %d", len(shard.Val), len(shard.Jc), len(shard.Ir), shard.M)
	}
	for rowId := uint32(0); rowId < shard.M; rowId++ {
		for j := shard.Ir[rowId]; j < shard.Ir[rowId+1]; j++ {
			fmt.Printf("%d\t%d\t%f\n", rowId, shard.Jc[j], shard.Val[j])
		}
	}
	return nil
}

func saveResult(shard *pb.MatrixShard, config *bwmf.Config, path string) error {
	client, err := bwmf.GetFsClient(config)
	if err != nil {
		return fmt.Errorf("Failed getting filesystem.Client: %s", err)
	}
	return bwmf.SaveMatrixShard(client, shard, path)
}

func loadPbBuffer(config *bwmf.Config, path string) (*pb.MatrixShard, error) {
	client, err := bwmf.GetFsClient(config)
	if err != nil {
		return nil, fmt.Errorf("Failed getting filesystem.Client: %s", err)
	}
	return bwmf.LoadMatrixShard(client, path)
}
