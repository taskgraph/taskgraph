package etcdutil

import (
	"log"

	"github.com/coreos/go-etcd/etcd"
)

func TryOccupyTask(client *etcd.Client, name string, taskID uint64, connection string) bool {
	_, err := client.Create(HealthyPath(name, taskID), "health", 30)
	if err != nil {
		return false
	}

	_, err = client.Set(MakeTaskMasterPath(name, taskID), connection, 0)
	if err != nil {
		log.Fatal(err)
	}
	return true
}
