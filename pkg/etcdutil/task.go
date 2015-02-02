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

// getAddress will return the host:port address of the service taking care of
// the task that we want to talk to.
// Currently we grab the information from etcd every time. Local cache could be used.
// If it failed, e.g. network failure, it should return error.
func GetAddress(client *etcd.Client, name string, id uint64) (string, error) {
	resp, err := client.Get(TaskMasterPath(name, id), false, false)
	if err != nil {
		return "", err
	}
	return resp.Node.Value, nil
}

func SetJobStatus(client *etcd.Client, name string, status int) error {
	_, err := client.Set(JobStatusPath(name), "done", 0)
	return err
}
