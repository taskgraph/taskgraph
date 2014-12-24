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
//   /{app}/healthy/{taskID} -> tasks' healthy condition
//   /{app}/nodes/: register nodes under this directory
//   /{app}/nodes/{nodeID}/address -> scheme://host:port/{path(if http)}
//   /{app}/nodes/{nodeID}/ttl -> keep alive timeout
//   /{app}/FreeTasks/{taskID}

const (
	TasksDir       = "tasks"
	NodesDir       = "nodes"
	ConfigDir      = "config"
	FreeDir        = "FreeTasks"
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

func HealthyPath(appName string) string {
	return path.Join("/", appName, healthy)
}

func TaskHealthyPath(appName string, taskID uint64) string {
	return path.Join("/", appName, healthy, strconv.FormatUint(taskID, 10))
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

func ParentMetaPath(appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		TaskParentMeta)
}

func ChildMetaPath(appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		TaskChildMeta)
}
