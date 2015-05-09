package framework

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
)

// Master is guaranteed the first to run. It mainly does the following:
// - initialize internal structures.
// - set up etcd. Register master address. Set up layout.
// - start server. This is the user defined grpc server.
// - start event handling. Select and handle events, e.g. sending and receiving messages,
//   finishing job, etc.
// - run user task.

func (m *master) Start() {
	m.init()
	m.setupEtcd()
	m.startServer()
	go m.startEventHandling()
	m.runUserTask()
	m.stop()
}

func (m *master) init() {
	m.etcdClient = etcd.NewClient(m.etcdURL)
	m.stopChan = make(chan struct{})
}

func (m *master) setupEtcd() {
	// init layout
	// register master's addr
	m.etcdClient.Set(etcdutil.MasterPath(m.job), m.listener.Addr().String(), 0)
}

func (m *master) startServer() {
	s := m.task.CreateServer()
	go s.Serve(m.listener)
}

func (m *master) startEventHandling() {
	for {
		select {
		case <-m.stopChan:
			return
		}
	}
}

func (m *master) runUserTask() {
	m.task.Setup(m)
	m.task.Run(context.Background())
}

func (m *master) stop() {
	m.listener.Close()
	close(m.stopChan)
}
