package taskgraph

// MapreduceTask is a logic repersentation of a computing unit in mapreduce framework.
// Each task contain at least one Node.

type MapreduceTask interface {
	Task                    // task interface of task graph
	Emit(string, string)    // commit interface for mapper
	Collect(string, string) // commit interface for
	MapreduceConfiguration(MapreduceConfig)
}
