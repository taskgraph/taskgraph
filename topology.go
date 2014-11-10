package meritop

// The Topology will be implemented by the application.
// Each Topology might have many epochs. The topology of each epoch might be different.
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
