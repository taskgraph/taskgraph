package etcdutil

import (
	"path"
	"strconv"
)

// The directory layout we going to define in etcd:
//   /{app}/config -> application configuration
//   /{app}/epoch -> global value for epoch
//   /{app}/tasks/: register tasks under this directory
//   /{app}/tasks/{taskID}/{replicaID} -> pointer to nodes, 0 replicaID means master
//   /{app}/tasks/{taskID}/meta -> meta change notification
//   /{app}/healthy/{taskID} -> tasks' healthy condition
//   /{app}/nodes/: register nodes under this directory
//   /{app}/nodes/{nodeID}/address -> scheme://host:port/{path(if http)}
//   /{app}/nodes/{nodeID}/ttl -> keep alive timeout
//   /{app}/FreeTasks/{taskID}

// /{job}/master/{replicaID}
// /{job}/worker/{workerID}

const (
	TasksDir                 = "tasks"
	NodesDir                 = "nodes"
	ConfigDir                = "config"
	FreeDir                  = "freeTasks"
	Epoch                    = "epoch"
	Status                   = "status"
	TaskMaster               = "0"
	NodeAddr                 = "address"
	NodeTTL                  = "ttl"
	Healthy                  = "healthy"
	MapreduceNodeStatusDir   = "nodeStatus"
)

func EpochPath(appName string) string {
	return path.Join("/", appName, Epoch)
}

func JobStatusPath(appName string) string {
	return path.Join("/", appName, Status)
}

func HealthyPath(appName string) string {
	return path.Join("/", appName, Healthy)
}

func TaskHealthyPath(appName string, taskID uint64) string {
	return path.Join(HealthyPath(appName), strconv.FormatUint(taskID, 10))
}
func FreeTaskDir(appName string) string {
	return path.Join("/", appName, FreeDir)
}
func FreeTaskPath(appName, idStr string) string {
	return path.Join(FreeTaskDir(appName), idStr)
}

func TaskDirPath(appName string) string {
	return path.Join("/", appName, TasksDir)
}

func TaskMasterPath(appName string, taskID uint64) string {
	return path.Join("/", appName, TasksDir, strconv.FormatUint(taskID, 10), TaskMaster)
}

func MetaPath(appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		"meta")
}

func MasterPath(job string) string {
	return path.Join("/", job, "master/0")
}
func WorkerPath(job string, id uint64) string {
	return path.Join("/", job, strconv.FormatUint(id, 10))
}

// etcd API for mapreduce based on old framework API
func MapreduceNodeStatusDir(appName string, id uint64) string {
	return path.Join("/", appName, MapreduceWorkerStatusDir, strconv.FormatUint(id, 10))
}

func MapreduceNodeStatusPath(appName string, id uint64, attr) string {
	return path.Join(MapreduceWorkerStatusDir(appName, id), attr)
}
