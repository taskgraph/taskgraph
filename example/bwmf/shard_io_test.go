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
	m, n := uint32(2), uint32(3)

	oldShard := &pb.MatrixShard{
		IsSparse: false,
		M: m,
		N: n,
		Val: []float32 {0.70, 1.00, 0.90, 0.70, 0.80, 0.90},
	}

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

	if len(newShard.Val) != len(oldShard.Val) {
		t.Errorf("num of values wrong. Expected %d, actual %d", len(oldShard.Val), len(newShard.Val))
	}

	for i := uint32(0); i < m; i++ {
		for j := uint32(0); j < n; j++ {
			if newShard.Val[i*n+j] != oldShard.Val[i*n+j] {
				t.Errorf("M[%d][%d] incorrect. Mismatched old %f, new %f", i, j, oldShard.Val[i*n+j], newShard.Val[i*n+j])
			}
		}
	}

	client.Remove(path)
}
