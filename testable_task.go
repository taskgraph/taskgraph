package meritop

import "log"

type testableTask struct {
	id             uint64
	parentMetaChan chan struct{}
	childMetaChan  chan struct{}
}

func (t *testableTask) Init(taskID uint64, framework Framework, config Config) {}
func (t *testableTask) Exit()                                                  {}
func (t *testableTask) ParentRestart(parentID uint64)                          {}
func (t *testableTask) ChildRestart(childID uint64)                            {}
func (t *testableTask) ParentDie(parentID uint64)                              {}
func (t *testableTask) ChildDie(childID uint64)                                {}

func (t *testableTask) ParentMetaReady(parentID uint64, meta Metadata) {
	log.Printf("Task(%d): parent(%d) meta ready:", t.id, parentID)
	close(t.parentMetaChan)
}
func (t *testableTask) ChildMetaReady(childID uint64, meta Metadata) {
	log.Printf("Task(%d): child(%d) meta ready:", t.id, childID)
	close(t.childMetaChan)
}

func (t *testableTask) SetEpoch(epoch uint64) {}
func (t *testableTask) ServeAsParent(req Metadata) Metadata {
	panic("unimplemented")
}
func (t *testableTask) ServeAsChild(reg Metadata) Metadata {
	panic("unimplemented")
}
func (t *testableTask) ParentDataReady(req, response Metadata) {}
func (t *testableTask) ChildDataReady(req, response Metadata)  {}
