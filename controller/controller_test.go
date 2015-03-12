package controller

import (
	"strconv"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

// etcd needs to be initialized beforehand
func TestControllerInitEtcdLayout(t *testing.T) {
	etcdClient := etcd.NewClient([]string{"http://localhost:4001"})

	tests := []struct {
		name          string
		numberOfTasks uint64
	}{
		{"test-1", 2},
		{"test-2", 4},
	}

	for i, tt := range tests {
		c := New(tt.name, etcdClient, tt.numberOfTasks, []string{"Parents", "Children"})
		c.InitEtcdLayout()

		for taskID := uint64(0); taskID < tt.numberOfTasks; taskID++ {
			key := etcdutil.FreeTaskPath(c.name, strconv.FormatUint(taskID, 10))
			if _, err := etcdClient.Get(key, false, false); err != nil {
				t.Errorf("task %d: etcdClient.Get %v failed: %v", i, key, err)
			}
			key = etcdutil.MetaPath("Parents", c.name, taskID)
			if _, err := etcdClient.Get(key, false, false); err != nil {
				t.Errorf("task %d: etcdClient.Get %v failed: %v", i, key, err)
			}
			key = etcdutil.MetaPath("Children", c.name, taskID)
			if _, err := etcdClient.Get(key, false, false); err != nil {
				t.Errorf("task %d: etcdClient.Get %v failed: %v", i, key, err)
			}
		}

		c.DestroyEtcdLayout()
	}
}
