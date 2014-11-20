package meritop

import (
	"bytes"
	"fmt"
	"net"
	"testing"
)

func TestFrameworkFlagMetaReady(t *testing.T) {
	m := mustNewMember(t, "framework_test")
	m.Launch()
	defer m.Terminate(t)
	url := fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())

	pFromIDChan := make(chan uint64, 1)
	cFromIDChan := make(chan uint64, 1)
	pMetaChan := make(chan string, 1)
	cMetaChan := make(chan string, 1)
	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   0,
		task: &testableTask{
			idChan:   cFromIDChan,
			metaChan: cMetaChan,
		},
		topology: NewTreeTopology(2, 1),
		ln:       createListener(t),
	}
	f1 := &framework{
		name:     "framework_test_flagmetaready",
		etcdURLs: []string{url},
		taskID:   1,
		task: &testableTask{
			idChan:   pFromIDChan,
			metaChan: pMetaChan,
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
		// from child(1)'s view
		if parentID := <-pFromIDChan; parentID != 0 {
			t.Errorf("#%d: parentID want = 0, get = %d", parentID)
		}
		if fromParent := <-pMetaChan; fromParent != tt.cMeta {
			t.Errorf("#%d: meta want = %s, get = %s", i, tt.cMeta, fromParent)
		}

		// 1: F#FlagParentMetaReady -> 0: T#ChildMetaReady
		f1.FlagParentMetaReady(tt.pMeta)
		// from parent(0)'s view
		if childID := <-cFromIDChan; childID != 1 {
			t.Errorf("#%d: childID want = 1, get = %d", childID)
		}
		if fromChild := <-cMetaChan; fromChild != tt.pMeta {
			t.Errorf("#%d: meta want = %s, get = %s", i, tt.pMeta, fromChild)
		}
	}
}

func TestFrameworkDataRequest(t *testing.T) {
	tests := []struct {
		req  string
		resp []byte
	}{
		{"request", []byte("response")},
		{"parameters", []byte{1, 2, 3}},
		{"gradient", []byte{4, 5, 6}},
	}

	dataMap := make(map[string][]byte)
	for _, tt := range tests {
		dataMap[tt.req] = tt.resp
	}

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

	pIDChan := make(chan uint64, 1)
	cIDChan := make(chan uint64, 1)
	pReqChan := make(chan string, 1)
	cReqChan := make(chan string, 1)
	pDataChan := make(chan []byte, 1)
	cDataChan := make(chan []byte, 1)
	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     "framework_test_datarequest",
		etcdURLs: []string{url},
		taskID:   0,
		task: &testableTask{
			idChan:   cIDChan,
			reqChan:  cReqChan,
			dataChan: cDataChan,
			dataMap:  dataMap,
		},
		topology:   NewTreeTopology(2, 1),
		ln:         l0,
		addressMap: addressMap,
	}
	f1 := &framework{
		name:     "framework_test_datarequest",
		etcdURLs: []string{url},
		taskID:   1,
		task: &testableTask{
			idChan:   pIDChan,
			reqChan:  pReqChan,
			dataChan: pDataChan,
			dataMap:  dataMap,
		},
		topology:   NewTreeTopology(2, 1),
		ln:         l1,
		addressMap: addressMap,
	}
	f0.start()
	defer f0.stop()
	f1.start()
	defer f1.stop()

	for i, tt := range tests {
		// 0: F#DataRequest -> 1: T#ServeAsChild -> 0: T#ChildDataReady
		f0.DataRequest(1, tt.req)
		// from child(1)'s view
		if id := <-pIDChan; id != 0 {
			t.Errorf("#%d: fromID want = 0, get = %d", i, id)
		}
		if req := <-pReqChan; req != tt.req {
			t.Errorf("#%d: req want = %s, get = %s", i, tt.req, req)
		}
		// from parent(0)'s view
		if id := <-cIDChan; id != 1 {
			t.Errorf("#%d: fromID want = 1, get = %d", i, id)
		}
		if req := <-cReqChan; req != tt.req {
			t.Errorf("#%d: req want = %s, get = %s", i, tt.req, req)
		}
		if data := <-cDataChan; bytes.Compare(data, tt.resp) != 0 {
			t.Errorf("#%d: resp want = %v, get = %v", i, tt.resp, data)
		}

		// 1: F#DataRequest -> 0: T#ServeAsParent -> 1: T#ParentDataReady
		f1.DataRequest(0, tt.req)
		// from parent(0)'s view
		if id := <-cIDChan; id != 1 {
			t.Errorf("#%d: fromID want = 1, get = %d", i, id)
		}
		if req := <-cReqChan; req != tt.req {
			t.Errorf("#%d: req want = %s, get = %s", i, tt.req, req)
		}
		// from child(1)'s view
		if id := <-pIDChan; id != 0 {
			t.Errorf("#%d: fromID want = 1, get = %d", i, id)
		}
		if req := <-pReqChan; req != tt.req {
			t.Errorf("#%d: req want = %s, get = %s", i, tt.req, req)
		}
		if data := <-pDataChan; bytes.Compare(data, tt.resp) != 0 {
			t.Errorf("#%d: resp want = %v, get = %v", i, tt.resp, data)
		}
	}
}

type testableTask struct {
	id        uint64
	framework Framework
	idChan    chan uint64
	metaChan  chan string
	reqChan   chan string
	dataChan  chan []byte
	dataMap   map[string][]byte
}

func (t *testableTask) Init(taskID uint64, framework Framework, config Config) {
	t.id = taskID
	t.framework = framework
}
func (t *testableTask) Exit()                 {}
func (t *testableTask) SetEpoch(epoch uint64) {}

func (t *testableTask) ParentMetaReady(fromID uint64, meta string) {
	if t.idChan != nil {
		t.idChan <- fromID
	}
	if t.metaChan != nil {
		t.metaChan <- meta
	}
}
func (t *testableTask) ChildMetaReady(fromID uint64, meta string) {
	if t.idChan != nil {
		t.idChan <- fromID
	}
	if t.metaChan != nil {
		t.metaChan <- meta
	}
}

func (t *testableTask) ServeAsParent(fromID uint64, req string) []byte {
	if t.idChan != nil {
		t.idChan <- fromID
	}
	if t.reqChan != nil {
		t.reqChan <- req
	}
	return t.dataMap[req]
}
func (t *testableTask) ServeAsChild(fromID uint64, req string) []byte {
	if t.idChan != nil {
		t.idChan <- fromID
	}
	if t.reqChan != nil {
		t.reqChan <- req
	}
	return t.dataMap[req]
}
func (t *testableTask) ParentDataReady(fromID uint64, req string, resp []byte) {
	if t.idChan != nil {
		t.idChan <- fromID
	}
	if t.reqChan != nil {
		t.reqChan <- req
	}
	if t.dataChan != nil {
		t.dataChan <- resp
	}
}

func (t *testableTask) ChildDataReady(fromID uint64, req string, resp []byte) {
	if t.idChan != nil {
		t.idChan <- fromID
	}
	if t.reqChan != nil {
		t.reqChan <- req
	}
	if t.dataChan != nil {
		t.dataChan <- resp
	}
}

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
