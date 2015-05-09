package framework

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

// Worker should register its address in etcd so that others can find him.
func TestWorkerSetupEtcd(t *testing.T) {
	job := "TestWorkerSetupEtcd"
	id := uint64(1)
	etcdClient := etcd.NewClient([]string{"http://localhost:4001"})
	w := &worker{
		job:        job,
		etcdClient: etcdClient,
		listener:   createListener(t),
		id:         id,
	}
	w.setupEtcd()
	resp, err := etcdClient.Get(etcdutil.WorkerPath(job, id), false, false)
	if err != nil {
		t.Fatalf("etcdClient.Get failed: %v", err)
	}
	addr := w.listener.Addr().String()
	if resp.Node.Value != addr {
		t.Fatalf("Wrong address on etcd: want = %s, get = %s", addr, resp.Node.Value)
	}
}
