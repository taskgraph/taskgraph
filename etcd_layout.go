package meritop

import (
	"path"
	"strconv"
)

// The directory layout we going to define in etcd:
//   /{app}/config -> application configuration
//   /{app}/tasks/: register tasks under this directory
//   /{app}/tasks/{taskID}/master -> pointer to nodes
//   /{app}/tasks/{taskID}/slave -> pointer to nodes
//   /{app}/tasks/{taskID}/parentMeta
//   /{app}/tasks/{taskID}/childMeta
//   /{app}/nodes/: register nodes under this directory
//   /{app}/nodes/{nodeID}/address -> scheme://host:port/{path(if http)}
//   /{app}/nodes/{nodeID}/ttl -> keep alive timeout
const (
	TasksDir       = "tasks"
	NodesDir       = "nodes"
	ConfigDir      = "config"
	TaskMaster     = "master"
	TaskSlave      = "slave"
	TaskParentMeta = "ParentMeta"
	TaskChildMeta  = "ChildMeta"
	NodeAddr       = "address"
	NodeTTL        = "ttl"
)

func MakeTaskMasterPath(appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		TaskMaster)
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
