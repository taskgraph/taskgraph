package bwmf

import (
	"testing"

	utl "github.com/taskgraph/taskgraph/example/bwmf/demo"
)

type dummyFramework struct {
}

func TestInitialization(t *testing.T) {
}

func TestUpdatingTShard(t *testing.T) {
	r1, c1, r2, c2, err := utl.getBufs()
}

func TestUpdatingDShard(t *testing.T) {
}
