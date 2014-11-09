package meritop

type Topology interface {
	// This should be called by framework at beginning of each epoch.

	// GetParents returns the parents' IDs of the given taskID at the
	// given epoch.
	GetParents(epochID, taskID uint64) []uint64
	// GetChlidren returns the children's IDs of the given taskID at the
	// given epoch.
	GetChildren(epochID, taskID uint64) []uint64
}
