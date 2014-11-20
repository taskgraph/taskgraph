package meritop

import (
	"fmt"
	"log"
	"net"
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
			cMetaChan: cMetaChan,
		},
		topology: NewTreeTopology(2, 1),
		ln:       createListener(t),
	}
	f1 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   1,
		task: &testableTask{
			pMetaChan: pMetaChan,
		},
		topology: NewTreeTopology(2, 1),
		ln:       createListener(t),
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

func TestFrameworkDataRequest(t *testing.T) {
	m := mustNewMember(t, "framework_test")
	m.Launch()
	defer m.Terminate(t)
	url := fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())
	l0 := createListener(t)
	l1 := createListener(t)
	addressMap := map[uint64]string{
		0: l0.Addr().String(),
		1: l1.Addr().String(),
	}
	serveAsParentChan := make(chan string, 1)
	serveAsChildChan := make(chan string, 1)
	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   0,
		task: &testableTask{
			serveAsParentChan: serveAsParentChan,
		},
		topology:   NewTreeTopology(2, 1),
		ln:         l0,
		addressMap: addressMap,
	}
	f1 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   1,
		task: &testableTask{
			serveAsChildChan: serveAsChildChan,
		},
		topology:   NewTreeTopology(2, 1),
		ln:         l1,
		addressMap: addressMap,
	}
	f0.start()
	defer f0.stop()
	f1.start()
	defer f1.stop()

	// 0: F#DataRequest -> 1: T#ServeAsChild -> 0: T#ChildDataReady
	f0.DataRequest(1, "test")
	<-serveAsChildChan
	// 1: F#DataRequest -> 0: T#ServeAsParent -> 1: T#ParentDataReady
	f1.DataRequest(0, "test")
	<-serveAsParentChan
}

type testableTask struct {
	id                uint64
	framework         Framework
	pMetaChan         chan string
	cMetaChan         chan string
	serveAsChildChan  chan string
	serveAsParentChan chan string
	dataMap           map[string][]byte
}

func (t *testableTask) Init(taskID uint64, framework Framework, config Config) {
	t.id = taskID
	t.framework = framework
}
func (t *testableTask) Exit()                 {}
func (t *testableTask) SetEpoch(epoch uint64) {}

func (t *testableTask) ParentMetaReady(parentID uint64, meta string) {
	log.Printf("Task(%d): parent(%d) meta ready.", t.id, parentID)
	if t.pMetaChan != nil {
		t.pMetaChan <- meta
	}
}
func (t *testableTask) ChildMetaReady(childID uint64, meta string) {
	log.Printf("Task(%d): child(%d) meta ready.", t.id, childID)
	if t.cMetaChan != nil {
		t.cMetaChan <- meta
	}
}

func (t *testableTask) ServeAsParent(fromID uint64, req string) []byte {
	log.Printf("Task(%d) received request from Child(%d) ", t.id, fromID)
	if t.serveAsParentChan != nil {
		t.serveAsParentChan <- req
	}
	return nil
}
func (t *testableTask) ServeAsChild(fromID uint64, req string) []byte {
	log.Printf("Task(%d) received request from parent(%d) ", t.id, fromID)
	if t.serveAsChildChan != nil {
		t.serveAsChildChan <- req
	}
	return nil
}
func (t *testableTask) ParentDataReady(parentID uint64, req string, resp []byte) {
}
func (t *testableTask) ChildDataReady(childID uint64, req string, resp []byte) {
}

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
