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
//   /{app}/tasks/{taskID}/parentMeta
//   /{app}/tasks/{taskID}/childMeta
//   /{app}/tasks/{taskID}/healthy
//   /{app}/nodes/: register nodes under this directory
//   /{app}/nodes/{nodeID}/address -> scheme://host:port/{path(if http)}
//   /{app}/nodes/{nodeID}/ttl -> keep alive timeout
//   /{app}/failedTasks/{taskID}

const (
	TasksDir       = "tasks"
	NodesDir       = "nodes"
	ConfigDir      = "config"
	FailedDir      = "failedTasks"
	Epoch          = "epoch"
	TaskMaster     = "0"
	TaskParentMeta = "ParentMeta"
	TaskChildMeta  = "ChildMeta"
	NodeAddr       = "address"
	NodeTTL        = "ttl"
	healthy        = "healthy"
)

func EpochPath(appName string) string {
	return path.Join("/", appName, Epoch)
}

func HealthyPath(appName string, taskID uint64) string {
	return path.Join("/", appName, TasksDir, strconv.FormatUint(taskID, 10), healthy)
}
func FailedTaskDir(appName string) string {
	return path.Join("/", appName, FailedDir)
}

func FailedTaskPath(appName string, taskID uint64) string {
	return path.Join(FailedTaskDir(appName), strconv.FormatUint(taskID, 10))
}

func MakeTaskDirPath(appName string) string {
	return path.Join("/", appName, TasksDir)
}

func MakeTaskMasterPath(appName string, taskID uint64) string {
	return path.Join("/", appName, TasksDir, strconv.FormatUint(taskID, 10), TaskMaster)
}

func MakeParentMetaPath(appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		TaskParentMeta)
}

func MakeChildMetaPath(appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		TaskChildMeta)
}
