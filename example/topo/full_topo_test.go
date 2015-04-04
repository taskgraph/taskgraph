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
	n := topo.GetNeighbors("Neighbors", 0)
	if len(n) != 3 {
		t.Error()
	}
	if n[0] != 0 {
		t.Error()
	}
	if n[1] != 1 {
		t.Error()
	}
	if n[2] != 2 {
		t.Error()
	}
	m := topo.GetNeighbors("Master", 0)
	if len(m) != 1 {
		t.Error()
	}
	if m[0] != 0 {
		t.Error()
	}
}
