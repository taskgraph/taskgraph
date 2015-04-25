package bwmf

import (
	"fmt"
	"testing"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
)

func TestShardIO(t *testing.T) {
	client := filesystem.NewLocalFSClient()
	path := "./.testShardIO.text.dat"

	oldShard := &pb.MatrixShard{
		Row: []*pb.MatrixShard_RowData{
			&pb.MatrixShard_RowData{At: make(map[int32]float32)},
			&pb.MatrixShard_RowData{At: make(map[int32]float32)},
		},
	}

	oldShard.Row[0].At[0] = 0.70
	oldShard.Row[0].At[1] = 1.00
	oldShard.Row[0].At[2] = 0.90
	oldShard.Row[1].At[1] = 0.70
	oldShard.Row[1].At[2] = 0.80
	oldShard.Row[1].At[3] = 0.90

	client.Remove(path)
	sErr := SaveMatrixShard(client, oldShard, path)
	if sErr != nil {
		t.Errorf("Saving shard failed: %s", sErr)
	}

	newShard, lErr := LoadMatrixShard(client, path)
	if sErr != nil {
		t.Errorf("Loading shard failed: %s", lErr)
	}

	// shard should be:
	// -----------------------------
	// | 0.70 | 1.00 | 0.90 | 0.00 |
	// -----------------------------
	// | 0.00 | 0.70 | 0.80 | 0.90 |
	// -----------------------------
	fmt.Println("Original matrix shard is: ", oldShard)
	fmt.Println("Loaded matrix shard is: ", newShard)

	if len(newShard.GetRow()) != len(oldShard.GetRow()) {
		t.Errorf("num of rows wrong. Expected %d, actual %d", len(oldShard.GetRow()), len(newShard.GetRow()))
	}

	for i := 0; i < 2; i++ {
		for k, v := range oldShard.Row[i].At {
			if newShard.Row[i].At[k] != v {
				t.Errorf("M[%d][%d] incorrect. Mismatched old %f, new %f", i, k, v, newShard.Row[i].At[k])
			}
		}
		for k, v := range newShard.Row[i].At {
			if oldShard.Row[i].At[k] != v {
				t.Errorf("M[%d][%d] incorrect. Mismatched old %f, new %f", i, k, v, oldShard.Row[i].At[k])
			}
		}
	}
}
