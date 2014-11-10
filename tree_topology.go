/*
The tree structure is basically assume that all the task forms a tree.
Also the tree structure stays the same between epochs.
*/
package meritop


type TreeTopology struct {
	fanout, numberOfTasks uint64
	taskID uint64
	parents, children []uint64
}


func (t TreeTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID

	// Not the most efficient way to create parents and children, but
	// since this is not on critical path, we are ok.
	var parents = make([]int64, 0, 1)
	var children = make([]uint64, 0, fanout)
	for index := 1; index < numberOfTasks; ++index {
		parents_indexes[index] = (index - 1)/fanout	
		var parentID = (index - 1)/fanout
		if (index == taskID) {
			parents = append(parents, parentID)
		}
		if (parentID == taskID) {
			children = append(children, index)
		}
	}
 }

func (t TreeTopology) GetParents(epochID uint64) []uint64 {
	return t.parents		
}

func (t TreeTopology) GetChildren(epochID uint64) []uint64 {
	return t.children	
}

// This simply append the new element to end.
func append(slice []uint64, element uint64) []uint64 {
    n := len(slice)
    slice = slice[0 : n+1]
    slice[n] = element
    return slice
}

// Creates a new tree topology with given fanout and number of tasks.
// This will be called each 
function NewTreeTopology(fanout, numberOfTasks uint64) *TreeTopology {
    m := new(TreeTopology)
    m.fanout = fanout
    m.numberOfTasks = numberOfTasks;
    return m
}