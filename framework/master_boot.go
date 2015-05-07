package framework

import "golang.org/x/net/context"

// Master is guaranteed the first to run. It does the following:
// - start a (etcd) controller.
// - set up server to be listening
// - set up message handling
// - run user task

func (m *master) Start() {
	//
	m.task.Setup(m)
	m.task.Run(context.Background())
}
