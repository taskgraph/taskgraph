/*
The topology is a data structure implemented by application, and
used by framework implementation to setup/manage the event according to
a set of topology defined here.

Each Topology describe a specific linkType in overall topology.
After SetTaskID(), GetNeighbors() return the result of this relationship of this taskID
For instance, a topology describe a master link, it will instruct
the framework the master link of node in each epoch.

The main usage is:
a. At beginning of the framework initialization for each task, framework
   call the SetTaskID so that the singleton Topology knows which taskID it
   represents.
b. At beginning of each epoch, the framework implementation call GetNeighbors
   with given epoch, so that it knows how to set watcher for node failure
   for specific relationship
*/

package taskgraph

// The Topology will be implemented by the application.
// Each Topology might have many epochs. The topology of each epoch
// might be different.
type Topology interface {
	// This method is called once by framework implementation. So that
	// we can get the local topology for each epoch later.
	SetTaskID(taskID uint64)

	// This returns the neighbors of given link for this node at this epoch.
	GetNeighbors(epoch uint64) []uint64
}
