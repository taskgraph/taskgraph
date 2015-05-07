package mapreduce

import (
	"github.com/taskgraph/taskgraph"
)

// MapreduceTask is a logic repersentation of a computing unit in mapreduce framework.
// Each task contains at least one Node.

type MapreduceTask interface {
	// task interface of mapreduce framework
	taskgraph.Task
	// For mapper work, after processing by function of user
	// it need provide user mapper function a method to emit their result to shuffle,
	// Thus framework could take these data to specfic shuffle.
	Emit(string, string)
	// As same as emit logic, it need provide user reduce function apu
	Collect(string, string)
}
