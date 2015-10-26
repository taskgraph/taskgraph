package topo

import "testing"

type treeLinkTypeTopoTest struct {
	id                uint64
	parents, children []uint64
}

//     0
//   1   2
//  3 4 5 6
// 7
func TestTreeLinkTypeToplogy27(t *testing.T) {
	tests27 := []treeLinkTypeTopoTest{
		{
			uint64(0),
			[]uint64{}, []uint64{1, 2},
		},
		{
			uint64(1),
			[]uint64{0}, []uint64{3, 4},
		},
		{
			uint64(3),
			[]uint64{1}, []uint64{7},
		},
		{
			uint64(5),
			[]uint64{2}, []uint64{},
		},
	}
	testTreeLinkTypeTopology(2, 8, tests27, t)
}

//     0
//   1   2
//  3 4 5 6
// 7 8
func TestTreeLinkTypeToplogy28(t *testing.T) {
	tests28 := []treeLinkTypeTopoTest{
		{
			uint64(0),
			[]uint64{}, []uint64{1, 2},
		},
		{
			uint64(1),
			[]uint64{0}, []uint64{3, 4},
		},
		{
			uint64(3),
			[]uint64{1}, []uint64{7, 8},
		},
	}
	testTreeLinkTypeTopology(2, 9, tests28, t)
}

func testTreeLinkTypeTopology(fanout, number uint64, tests []treeLinkTypeTopoTest, t *testing.T) {
	for _, tt := range tests {
		// For each row, we construct the tree topology and then
		// set up the task id.
		treeParentTopology := NewTreeTopologyOfParent(fanout, number)
		treeParentTopology.SetTaskID(tt.id)

		parents := treeParentTopology.GetNeighbors(0)
		if len(parents) != len(tt.parents) {
			t.Errorf("TreeTopology27 got wrong number of parents for %q", tt.id)
		}
		for index, element := range parents {
			if element != tt.parents[index] {
				t.Errorf("Mismatch in %qth parent: expected %q got %q", index, element, tt.parents[index])
			}
		}

		treeChildrenTopology := NewTreeTopologyOfChildren(fanout, number)
		treeChildrenTopology.SetTaskID(tt.id)

		children := treeChildrenTopology.GetNeighbors(0)
		if len(children) != len(tt.children) {
			t.Errorf("TreeTopology27 got wrong number of children for %q", tt.id)
		}
		for index, element := range children {
			if element != tt.children[index] {
				t.Errorf("Mismatch in %qth children: expected %q got %q", index, element, tt.children[index])
			}
		}
	}
}
