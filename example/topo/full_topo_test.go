package topo

import (
	"testing"
)

func TestFullTopogoly(t *testing.T) {
	topo := NewFullTopology(3)

	topo.SetTaskID(1)

	if topo.numOfTasks != 3 {
		t.Error()
	}
	if topo.taskID != 1 {
		t.Error()
	}
	if len(topo.neighbors) != 2 {
		t.Error()
	}
	if topo.neighbors[0] != 0 {
		t.Error()
	}
	if topo.neighbors[1] != 2 {
		t.Error()
	}
}
