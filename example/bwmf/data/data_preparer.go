package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
)

// XXX(baigang): Matrix shard data converter.
// This is for converting a text representation of sparse matrix into our proto defined matrix object and save the serialized/marshalled buffer onto HDFS.
// The text representation formats as:
//   rowId \t columnId \s freq \s columnId \s freq ...
// RowID and values are separated by a tab, and column/value pairs are separated by a space.
func main() {

	hdfsNamenodeAddr := flag.String("namenode", "", "name node host.")
	webhdfsAddr := flag.String("webhdfs", "", "web hdfs host.")
	hdfsUser := flag.String("hdfsuser", "", "hdfs user name.")
	savePath := flag.String("output", "", "output path on HDFS.")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	mat := &pb.SparseMatrixShard{
		Row: make([]*pb.SparseMatrixShard_SparseRow, 0, 256),
	}

	fmt.Println("namenode: ", *hdfsNamenodeAddr)
	fmt.Println("webhdfs: ", *webhdfsAddr)
	fmt.Println("hdfsuser: ", *hdfsUser)
	fmt.Println("output: ", *savePath)

	// set up hdfs
	hdfsClient, fsErr := filesystem.NewHdfsClient(
		*hdfsNamenodeAddr,
		*webhdfsAddr,
		*hdfsUser,
	)
	if fsErr != nil {
		fmt.Println("Failed creating HDFS client: ", fsErr)
		return
	}

	writer, oErr := hdfsClient.OpenWriteCloser(*savePath)
	if oErr != nil {
		fmt.Println("Failed open output file: ", *savePath, " with Error ", oErr)
		return
	}

	// load matrix
	for scanner.Scan() {
		line := scanner.Text()
		if line == "\t" {
			continue
		}
		lineReader := strings.NewReader(line)
		rowId := -1
		_, sErr := fmt.Fscanf(lineReader, "%d", &rowId)
		if sErr != nil {
			fmt.Printf("Failed scanning rowid: %s", sErr)
			return
		}
		// fmt.Println("row id is ", rowId)
		row := &pb.SparseMatrixShard_SparseRow{
			At: make(map[int32]float32),
		}
		for sErr == nil {
			colId, freq := -1, -1
			_, sErr = fmt.Fscanf(lineReader, "%d %d", &colId, &freq)
			if sErr == nil {
				// fmt.Printf("Scanned colId %d freq %d \n", colId, freq)
				row.At[int32(colId)] = float32(freq)
			}
		}
		mat.Row = append(mat.Row, row)
	}

	buf, mErr := proto.Marshal(mat)
	if mErr != nil {
		fmt.Printf("Failed marshalling data: %s", mErr)
	}

	s, wErr := writer.Write(buf)
	if wErr != nil {
		fmt.Println("Failed saving results to output path ", *savePath, " with Error ", wErr)
		return
	}
	fmt.Println("Successfully saved ", s, " bytes to ", *savePath)
}
