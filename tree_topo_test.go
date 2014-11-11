package meritop

import "testing"

type testInfo struct {
	in         		  uint64
	parents, children []uint64
}

func TestTreeToplogy27(t *testing.T) {
	tests27 := []struct{
		in                uint64
		parents, children []uint64
	}{
		{
			uint64(0),
			[]uint64{}, []uint64{1, 2}
		},
		{
			uint64(1),
			[]uint64{0}, []uint64{3, 4}
		},
		{
			uint64(5),
			[]uint64{2}, []uint64{}
		},
	}
	TreeTopologyTesting(tests27, t)
}

func TestTreeToplogy28(t *testing.T) {
	tests28 := []struct{
    	in 				  uint64
    	parents, children []uint64
	}{
		{
			uint64(0),
			[]uint64{}, []uint64{1, 2}
		},
		{
			uint64(1),
			[]uint64{0}, []uint64{3, 4}
		},
		{
			uint64(3),
			[]uint64{1}, []uint64{7}
		},
	}
	TreeTopologyTesting(tests28, t)
}

func TreeTopologyTesting(tests []testInfo, t *testing.T) {
	for _, tt := range tests {
		// For each row, we construct the tree topology and then
		// set up the task id.
		treeTopology := NewTreeTopology(2, 7)
		treeTopology.SetTaskID(tt.in)

		parents := treeTopology.GetParents(0)
		if len(parents) != len(tt.parents) {
			t.Errorf("TreeTopology27 got wrong number of parents for %q", tt.in)
		}
		for index, element := range parents {
			if element != tt.parents[index] {
				t.Errorf("Mismatch in %qth parent: expected %q got %q", index, element, tt.parents[index])
			}
		}

		children := treeTopology.GetChildren(0)
		if len(children) != len(tt.children) {
			t.Errorf("TreeTopology27 got wrong number of children for %q", tt.in)
		}
		for index, element := range children {
			if element != tt.children[index] {
				t.Errorf("Mismatch in %qth children: expected %q got %q", index, element, tt.children[index])
			}
		}
	}
}
