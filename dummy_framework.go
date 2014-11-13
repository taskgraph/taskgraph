package meritop

import "log"

type dummyFramework struct {
}

func (f *dummyFramework) FlagParentMetaReady(meta Metadata) {
}

func (f *dummyFramework) FlagChildMetaReady(meta Metadata) {
}

func (f *dummyFramework) GetTopology() Topology {
	panic("unimplemented")
}

func (f *dummyFramework) Exit() {
}

func (f *dummyFramework) SetEpoch(epochID uint64) {
}

func (f *dummyFramework) GetLogger() log.Logger {
	panic("unimplemented")
}

func (f *dummyFramework) DataRequest(toID uint64, meta Metadata) {
}

func (f *dummyFramework) GetNode(taskID uint64) Node {
	panic("unimplemented")
}
func (f *dummyFramework) HasChildren() bool {
	panic("unimplemented")
}
func (f *dummyFramework) HasParents() bool {
	panic("unimplemented")
}
func (f *dummyFramework) GetTaskID() uint64 {
	panic("unimplemented")
}
