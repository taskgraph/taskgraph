package topo

import (
	"testing"
)

func TestFullTopogoly(t *testing.T) {
	topo := NewFullTopology(2)

	topo.SetTaskID(0)

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
	if len(m) != 1 {
		t.Error()
	}
	if n[0] != 0 {
		t.Error()
	}
}
