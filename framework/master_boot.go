package framework

import (
	"github.com/coreos/go-etcd/etcd"
	"golang.org/x/net/context"
)

// Master is guaranteed the first to run. It mainly does the following:
// - initialize internal structures.
// - set up etcd. Register master address. Set up layout.
// - start server. This is the user defined grpc server.
// - start event handling. Select and handle events, e.g. sending and receiving messages,
//   finishing job, etc. are events.
// - run user task.

func (m *master) Start() {
	m.init()
	m.setupEtcd()
	m.startServer()
	go m.startEventHandling()
	m.runUserTask()
}

func (m *master) init() {
	m.etcdClient = etcd.NewClient(m.etcdURL)
}

func (m *master) setupEtcd() {
	// init layout
	// register address for master
}

func (m *master) startServer() {
	s := m.task.CreateServer()
	go s.Serve(m.listener)
}

func (m *master) startEventHandling() {
	for {
		select {}
	}
}

func (m *master) runUserTask() {
	m.task.Setup(m)
	m.task.Run(context.Background())
}
