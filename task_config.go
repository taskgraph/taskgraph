/*
TaskConfig is also implemented by application developer and used by
framework implementation so decide which task implementation one should use
at any give node. It should be called only once at node initialization.
To use this:
first, push all the task implementations into a slice
second, use this to figure out which implementation should be used for given node.
*/
package meritop

type TaskConfig interface {
	// This method is called once by framework implementation to get the
	// right task implementation for the node/task. It requires the taskID
	// for current node, and also a global array of tasks.
	GetTask(taskID uint64, tasks []Task) Task
}
