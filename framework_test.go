package meritop

import (
	"fmt"
	"log"
	"testing"
)

func TestFrameworkFlagMetaReady(t *testing.T) {
	m := mustNewMember(t, "framework_test")
	m.Launch()
	defer m.Terminate(t)
	url := fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())

	pMetaChan := make(chan string, 1)
	cMetaChan := make(chan string, 1)
	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   0,
		task: &testableTask{
			pMetaChan: nil,
			cMetaChan: cMetaChan,
		},
		topology: NewTreeTopology(2, 1),
	}
	f1 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   1,
		task: &testableTask{
			pMetaChan: pMetaChan,
			cMetaChan: nil,
		},
		topology: NewTreeTopology(2, 1),
	}

	f0.start()
	defer f0.stop()
	f1.start()
	defer f1.stop()

	tests := []struct {
		cMeta string
		pMeta string
	}{
		{"parent", "child"},
		{"ParamReady", "GradientReady"},
	}

	for i, tt := range tests {
		// 0: F#FlagChildMetaReady -> 1: T#ParentMetaReady
		f0.FlagChildMetaReady(tt.cMeta)
		fromParent := <-pMetaChan
		if fromParent != tt.cMeta {
			t.Errorf("#%d: want = %s, get = %s", i, tt.cMeta, fromParent)
		}

		// 1: F#FlagParentMetaReady -> 0: T#ChildMetaReady
		f1.FlagParentMetaReady(tt.pMeta)
		fromChild := <-cMetaChan
		if fromChild != tt.pMeta {
			t.Errorf("#%d: want = %s, get = %s", i, tt.pMeta, fromChild)
		}
	}
}

type testableTask struct {
	id        uint64
	pMetaChan chan string
	cMetaChan chan string
}

func (t *testableTask) Init(taskID uint64, framework Framework, config Config) {
	t.id = taskID
}
func (t *testableTask) Exit()                 {}
func (t *testableTask) SetEpoch(epoch uint64) {}

func (t *testableTask) ParentMetaReady(parentID uint64, meta string) {
	log.Printf("Task(%d): parent(%d) meta ready:", t.id, parentID)
	t.pMetaChan <- meta
}
func (t *testableTask) ChildMetaReady(childID uint64, meta string) {
	log.Printf("Task(%d): child(%d) meta ready:", t.id, childID)
	t.cMetaChan <- meta
}

func (t *testableTask) ServeAsParent(req string) ([]byte, error) {
	panic("unimplemented")
}
func (t *testableTask) ServeAsChild(req string) ([]byte, error) {
	panic("unimplemented")
}
func (t *testableTask) ParentDataReady(parentID uint64, req string, resp []byte) {}
func (t *testableTask) ChildDataReady(childID uint64, req string, resp []byte)   {}
