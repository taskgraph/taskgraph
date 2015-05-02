package mapreduce

import (
	"testing"
)

func TestMapReduceTopology(t *testing.T) {
	topo := NewMapReduceTopology(3, 5, 2)
	linkTypes := topo.GetLinkTypes()
	if len(linkTypes) != 4 {
		t.Error()
	}
	topo.SetTaskID(1)

	if topo.NumOfMapper != 3 {
		t.Error()
	}

	if topo.NumOfShuffle != 5 {
		t.Error()
	}

	if topo.NumOfReducer != 2 {
		t.Error()
	}
	if topo.taskID != 1 {
		t.Error()
	}
	n := topo.GetNeighbors("Suffix", 0)
	if len(n) != 5 {
		t.Error()
	}
	if n[0] != 3 {
		t.Error()
	}
	if n[1] != 4 {
		t.Error()
	}
	if n[4] != 7 {
		t.Error()
	}

	topo.SetTaskID(0)

	if topo.taskID != 0 {
		t.Error()
	}

	m := topo.GetNeighbors("Suffix", 1)
	if len(m) != 1 {
		t.Error()
	}

	if m[0] != 5 {
		t.Error()
	}

	topo.SetTaskID(8)

	m = topo.GetNeighbors("Slave", 0)
	if len(m) != 8 {
		t.Error()
	}

	m = topo.GetNeighbors("Slave", 1)
	if len(m) != 8 {
		t.Error()
	}

	topo.SetTaskID(2)

	m = topo.GetNeighbors("Master", 1)
	if len(m) != 1 {
		t.Error()
	}
	if m[0] != 8 {
		t.Error()
	}

}
