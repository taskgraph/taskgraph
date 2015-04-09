package topo

import (
	"testing"
)

func TestFullTopogoly(t *testing.T) {
	topo := NewFullTopology(2)

	topo.SetTaskID(1)

	if topo.numOfTasks != 2 {
		t.Error()
	}
	if topo.taskID != 1 {
		t.Error()
	}
	n := topo.GetNeighbors("Neighbors", 0)
	if len(n) != 2 {
		t.Error()
	}
	if n[0] != 0 {
		t.Error()
	}
	if n[1] != 1 {
		t.Error()
	}

	m := topo.GetNeighbors("Master", 0)
	if len(m) != 0 {
		t.Error()
	}

	topo.SetTaskID(0)
	m = topo.GetNeighbors("Master", 0)
	if len(m) != 2 {
		t.Error()
	}
	if n[0] != 0 {
		t.Error()
	}
	if n[1] != 1 {
		t.Error()
	}
}
