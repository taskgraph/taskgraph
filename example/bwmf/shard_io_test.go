package bwmf

import (
	"fmt"
	"testing"
)

func TestLoadShard(t *testing.T) {
	conf := ioconfig{
		IFs: "local",
	}
	shard, ldErr := LoadSparseShard(conf, "./data/row_shard.dat.1")

	if ldErr != nil {
		t.Errorf("Loading shard failed: %s", ldErr)
	}

	fmt.Println("Loaded matrix shard is: ", shard)
	// shard should be:
	// -----------------------------
	// | 0.70 | 1.00 | 0.90 | 0.00 |
	// -----------------------------
	if len(shard.GetRow()) != 1 {
		t.Errorf("num of rows wrong. Expected 1, actual %d", len(shard.GetRow()))
	}
	if shard.GetRow()[0].At[0] != 0.70 {
		t.Errorf("M[0][0] incorrect. Expected 0.20, actual %f", shard.GetRow()[0].At[0])
	}
	if shard.GetRow()[0].At[1] != 1.00 {
		t.Errorf("M[0][0] incorrect. Expected 1.00, actual %f", shard.GetRow()[0].At[1])
	}
	if shard.GetRow()[0].At[2] != 0.90 {
		t.Errorf("M[0][0] incorrect. Expected 0.90, actual %f", shard.GetRow()[0].At[2])
	}
	if shard.GetRow()[0].At[3] != 0.00 {
		t.Errorf("M[0][0] incorrect. Expected 0, actual %f", shard.GetRow()[0].At[3])
	}
}
