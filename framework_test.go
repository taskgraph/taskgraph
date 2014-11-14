package meritop

import (
	"fmt"
	"testing"
)

func TestFrameworkFlagMetaReady(t *testing.T) {
	m := mustNewMember(t, "framework_test")
	m.Launch()
	defer m.Terminate(t)
	url := fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())

	parentMetaChan := make(chan struct{})
	childMetaChan := make(chan struct{})
	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   0,
		task: &testableTask{
			id:             0,
			parentMetaChan: nil,
			childMetaChan:  childMetaChan,
		},
		topology: NewTreeTopology(2, 1),
	}
	f1 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   1,
		task: &testableTask{
			id:             1,
			parentMetaChan: parentMetaChan,
			childMetaChan:  nil,
		},
		topology: NewTreeTopology(2, 1),
	}

	f0.start()
	defer f0.stop()
	f1.start()
	defer f1.stop()

	// 0: F#FlagChildMetaReady -> 1: T#ParentMetaReady
	f0.FlagChildMetaReady(nil)
	<-parentMetaChan

	// 1: F#FlagParentMetaReady -> 0: T#ChildMetaReady
	f1.FlagParentMetaReady(nil)
	<-childMetaChan
}
