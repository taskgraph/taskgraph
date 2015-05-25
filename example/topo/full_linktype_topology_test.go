package topo

import (
	"testing"
)

func TestFullLinkTypeTopogoly(t *testing.T) {
	// test topology of Neighbors Linktype
	topoNeighbor := NewFullTopologyOfNeighbor(2)

	topoNeighbor.SetTaskID(0)

	n := topoNeighbor.GetNeighbors(0)
	if len(n) != 2 {
		t.Error()
	}
	if n[0] != 0 {
		t.Error()
	}
	if n[1] != 1 {
		t.Error()
	}

	// test topology of Master Linktype
	topoMaster := NewFullTopologyOfMaster(2)

	topoMaster.SetTaskID(0)

	m := topoMaster.GetNeighbors(0)
	if len(m) != 1 {
		t.Error()
	}
	if n[0] != 0 {
		t.Error()
	}
}
