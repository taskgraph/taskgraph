package meritop

// The directory layout we going to define in etcd:
//   /{app}/config -> application configuration
//   /{app}/tasks/: register tasks under this directory
//   /{app}/tasks/{taskID}/master -> pointer to nodes
//   /{app}/tasks/{taskID}/slave -> pointer to nodes
//   /{app}/nodes/: register nodes under this directory
//   /{app}/nodes/{nodeID}/address -> scheme://host:port/{path(if http)}
//   /{app}/nodes/{nodeID}/ttl -> keep alive timeout
const (
	TasksDir     = "tasks"
	NodesDir     = "nodes"
	ConfigDir    = "config"
	TaskMaster   = "master"
	TaskSlave    = "slave"
	NodeInfoConn = "connect"
	NodeInfoTTL  = "ttl"
)
