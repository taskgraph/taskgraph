package topoutil

import "github.com/taskgraph/taskgraph"

func IsParent(t taskgraph.Topology, epoch, taskID uint64) bool {
	for _, id := range t.GetLinks("Parents", epoch) {
		if taskID == id {
			return true
		}
	}
	return false
}

func IsChild(t taskgraph.Topology, epoch, taskID uint64) bool {
	for _, id := range t.GetLinks("Children", epoch) {
		if taskID == id {
			return true
		}
	}
	return false
}
