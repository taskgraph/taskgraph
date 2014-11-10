/*
The topology is a data structure that implemented by application, and
used by frameework implementation to setup/manage the event according to
topology defined here. The main usage is:
a. At beginning of the framework initialization for each task, framework
   call the SetTaskID so that the singleton Topology knows which taskID it
   represents.
b. At beginning of each epock, the framework implementation (on each task)
   will call GetParents and GetChildren with given epoch, so that it know
   how to setup watcher for node failures. 
*/
package meritop

// The Topology will be implemented by the application.
// Each Topology might have many epochs. The topology of each epoch 
// might be different.
type Topology interface {
	// This method is called once by framework implementation. So that
	// we can get the local topology for each epoch later.
	SetTaskID(taskID uint64)

	// GetParents returns the parents' IDs of this task at the
	// given epoch.
	GetParents(epochID uint64) []uint64

	// GetChlidren returns the children's IDs of this task at the
	// given epoch.
	GetChildren(epochID uint64) []uint64
}
