package etcdutil

import (
	"strconv"
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

func TryOccupyTask(client *etcd.Client, name string, taskID uint64, connection string) (bool, error) {
	_, err := client.Create(TaskHealthyPath(name, taskID), "health", 3)
	if err != nil {
		if strings.Contains(err.Error(), "Key already exists") {
			return false, nil
		}
		return false, err
	}
	idStr := strconv.FormatUint(taskID, 10)
	client.Delete(FreeTaskPath(name, idStr), false)
	_, err = client.Set(TaskMasterPath(name, taskID), connection, 0)
	if err != nil {
		return false, err
	}
	return true, nil
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
	err := AtomicInc(client, JobStatusPath(name))
	return err
}
