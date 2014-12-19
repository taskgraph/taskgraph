package controller

import (
	"fmt"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

// etcd needs to be initialized beforehand
func TestControllerInitEtcdLayout(t *testing.T) {
	m := etcdutil.MustNewMember(t, "controller_test")
	m.Launch()
	defer m.Terminate(t)

	url := fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())

	etcdClient := etcd.NewClient([]string{url})

	tests := []struct {
		name          string
		numberOfTasks uint64
	}{
		{"test-1", 2},
		{"test-2", 4},
	}

	for i, tt := range tests {
		c := &Controller{
			name:       tt.name,
			etcdclient: etcdClient,
			numOfTasks: tt.numberOfTasks,
		}
		c.InitEtcdLayout()

		for taskID := uint64(0); taskID < tt.numberOfTasks; taskID++ {
			key := etcdutil.TaskMasterPath(tt.name, taskID)
			_, err := etcdClient.Get(key, false, false)
			if err != nil {
				t.Errorf("#%d: etcdClient.Get failed: %v", i, err)
			}
		}

		c.DestroyEtcdLayout()
	}
}
