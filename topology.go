package meritop

// The Topology will be implemented by the application.
// Each Topology might have many epochs. The topology of each epoch might be different.
type Topology interface {
	// GetParents returns the parents' IDs of the given taskID at the
	// given epoch.
	GetParents(epochID, taskID uint64) []uint64
	// GetChlidren returns the children's IDs of the given taskID at the
	// given epoch.
	GetChildren(epochID, taskID uint64) []uint64
}
