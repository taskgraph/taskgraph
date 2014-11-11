package meritop

type Node interface {
	// return the ID of this node
	ID() uint64
	// return the task this node associated to
	TaskID() uint64
	// return the status of this node
	// possible status: no associated to any task
	//                  master of a task
	//                  slave of a task
	Status() uint64
	// return a connection string of this node
	// scheme://host:port
	Connection() string
}
