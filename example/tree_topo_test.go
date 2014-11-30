package example

import "testing"

type treeTopoTest struct {
	id                uint64
	parents, children []uint64
}

//     0
//   1   2
//  3 4 5 6
// 7
func TestTreeToplogy27(t *testing.T) {
	tests27 := []treeTopoTest{
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
	testTreeTopology(2, 8, tests27, t)
}

//     0
//   1   2
//  3 4 5 6
// 7 8
func TestTreeToplogy28(t *testing.T) {
	tests28 := []treeTopoTest{
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
	testTreeTopology(2, 9, tests28, t)
}

func testTreeTopology(fanout, number uint64, tests []treeTopoTest, t *testing.T) {
	for _, tt := range tests {
		// For each row, we construct the tree topology and then
		// set up the task id.
		treeTopology := NewTreeTopology(fanout, number)
		treeTopology.SetTaskID(tt.id)

		parents := treeTopology.GetParents(0)
		if len(parents) != len(tt.parents) {
			t.Errorf("TreeTopology27 got wrong number of parents for %q", tt.id)
		}
		for index, element := range parents {
			if element != tt.parents[index] {
				t.Errorf("Mismatch in %qth parent: expected %q got %q", index, element, tt.parents[index])
			}
		}

		children := treeTopology.GetChildren(0)
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
