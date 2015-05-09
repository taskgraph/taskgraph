package framework

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
)

// Worker has similar workflow to master. Although they have different impl.

func (w *worker) Start() {
	w.init()
	w.setupEtcd()
	w.startServer()
	go w.startEventHandling()
	w.runUserTask()
	w.stop()
}

func (w *worker) init() {
	w.etcdClient = etcd.NewClient(w.etcdURL)
	w.stopChan = make(chan struct{})
}

func (w *worker) setupEtcd() {
	// register worker's addr
	w.etcdClient.Set(etcdutil.WorkerPath(w.job, w.id), w.listener.Addr().String(), 0)
}

func (w *worker) startServer() {
	s := w.task.CreateServer()
	go s.Serve(w.listener)
}

func (w *worker) startEventHandling() {
	for {
		select {
		case <-w.stopChan:
			return
		}
	}
}

func (w *worker) runUserTask() {
	w.task.Setup(w, w.id)
	w.task.Run(context.Background())
}

func (w *worker) stop() {
	w.listener.Close()
	close(w.stopChan)
}
