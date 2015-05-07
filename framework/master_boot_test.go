package framework

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

// Master should register its address in etcd so that workers can find him.
func TestMasterSetupEtcd(t *testing.T) {
	job := "TestMasterSetupEtcd"
	etcdClient := etcd.NewClient([]string{"http://localhost:4001"})
	m := &master{
		job:        job,
		etcdClient: etcdClient,
		listener:   createListener(t),
	}
	m.setupEtcd()
	resp, err := etcdClient.Get(etcdutil.MasterPath(job), false, false)
	if err != nil {
		t.Fatalf("etcdClient.Get failed: %v", err)
	}
	addr := m.listener.Addr().String()
	if resp.Node.Value != addr {
		t.Fatalf("Wrong address on etcd: want = %s, get = %s", addr, resp.Node.Value)
	}
}
