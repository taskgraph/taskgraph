package etcdutil

import (
	"log"
	"strconv"

	"github.com/coreos/go-etcd/etcd"
)

func TryOccupyTask(client *etcd.Client, name string, taskID uint64, connection string) bool {
	_, err := client.Create(TaskHealthyPath(name, taskID), "health", 3)
	if err != nil {
		return false
	}
	idStr := strconv.FormatUint(taskID, 10)
	client.Delete(FreeTaskPath(name, idStr), false)
	_, err = client.Set(TaskMasterPath(name, taskID), connection, 0)
	if err != nil {
		log.Fatal(err)
	}
	return true
}
