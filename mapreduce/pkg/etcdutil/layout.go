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
	FreeDir        = "freeTasks"
	Epoch          = "epoch"
	Status         = "status"
	TaskMaster     = "0"
	NodeAddr       = "address"
	NodeTTL        = "ttl"
	Healthy        = "healthy"
	FreeDirForWork = "freeWorks"
	WorkDir        = "work"
	WorkOccupyDir  = "occupyWork"
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

func MetaPath(linkType, appName string, taskID uint64) string {
	return path.Join("/",
		appName,
		TasksDir,
		strconv.FormatUint(taskID, 10),
		linkType)
}

func TaskMasterWork(appName, workStr string) string {
	return path.Join("/", appName, WorkDir, workStr)
}

func FreeWorkDir(appName string) string {
	return path.Join("/", appName, FreeDirForWork)
}

func FreeWorkPath(appName, workStr string) string {
	return path.Join(FreeWorkDir(appName), workStr)
}

func OccupyWorkPath(appName, workStr string) string {
	return path.Join(appName, WorkOccupyDir, workStr)
}

func TaskMasterWorkForType(appName, workType, idStr string) string {
	return path.Join("/", appName, WorkDir, idStr)
}

func OccupyWorkPathForType(appName, workType, idStr string) string {
	return path.Join(appName, WorkOccupyDir, workType, idStr)
}

func FreeWorkDirForType(appName, workDir string) string {
	return path.Join("/", appName, FreeDirForWork, workDir)
}

func FreeWorkPathForType(appName, workType, idStr string) string {
	return path.Join(FreeWorkDir(appName), workType, idStr)
}
