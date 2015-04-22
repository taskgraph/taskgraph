/*
TaskBuilder is also implemented by application developer and used by
framework implementation so decide which task implementation one should use
at any give node. It should be called only once at node initialization.
*/
package taskgraph

type MapreduceTaskBuilder interface {
	// This method is called once by framework implementation to get the
	// right task implementation for given node/task.
	GetTask(taskID uint64) MapreduceTask
}
