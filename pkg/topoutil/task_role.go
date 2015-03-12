package topoutil

import "github.com/taskgraph/taskgraph"

func IsParent(t taskgraph.Topology, epoch, taskID uint64) bool {
	for _, id := range t.GetNeighbors("Parents", epoch) {
		if taskID == id {
			return true
		}
	}
	return false
}

func IsChild(t taskgraph.Topology, epoch, taskID uint64) bool {
	for _, id := range t.GetNeighbors("Children", epoch) {
		if taskID == id {
			return true
		}
	}
	return false
}
