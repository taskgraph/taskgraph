package etcdutil

import (
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

func TryOccupyNode(freeNodePath, healthyPath string, setPath string, client *etcd.Client, connection string) (bool, error) {
	if healthyPath != "" {
		_, err := client.Create(healthyPath, "health", 3)
		if err != nil {
			if strings.Contains(err.Error(), "Key already exists") {
				return false, nil
			}
			return false, err
		}
	}
	//
	client.Delete(freeNodePath, false)
	_, err := client.Set(setPath, connection, 0)
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
	_, err := client.Set(JobStatusPath(name), "done", 0)
	return err
}
