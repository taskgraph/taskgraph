package meritop

import (
	"path"
	"strconv"
)

// The directory layout we going to define in etcd:
//   /{app}/config -> application configuration
//   /{app}/tasks/: register tasks under this directory
//   /{app}/tasks/{taskID}/index -> pointer to nodes, index = 0 means master
//   /{app}/tasks/{taskID}/parentMeta
//   /{app}/tasks/{taskID}/childMeta
//   /{app}/nodes/: register nodes under this directory
//   /{app}/nodes/{nodeID}/address -> scheme://host:port/{path(if http)}
//   /{app}/nodes/{nodeID}/ttl -> keep alive timeout

const (
	TasksDir       = "tasks"
	NodesDir       = "nodes"
	ConfigDir      = "config"
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
		"0")
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
