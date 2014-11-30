package topoutil

import "github.com/go-distributed/meritop"

func IsParent(t meritop.Topology, epoch, taskID uint64) bool {
	for _, id := range t.GetParents(epoch) {
		if taskID == id {
			return true
		}
	}
	return false
}

func IsChild(t meritop.Topology, epoch, taskID uint64) bool {
	for _, id := range t.GetChildren(epoch) {
		if taskID == id {
			return true
		}
	}
	return false
}
