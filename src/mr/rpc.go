package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "fmt"
import "os"
import "strconv"

type TaskType string
const (
	MapTask TaskType = "map"
	ReduceTask = "reduce"
	WaitTask = "wait"
	ExitTask = "exit" 
)

type Task struct {
	TaskType TaskType
	TaskId int

	// Fields for MapTask
	InputFileName string
	NReduce int

	// Fields for ReduceTask
	ReduceId int
}

func (t Task) String() string {
	return fmt.Sprintf("Task(TaskType: %s, TaskId: %d, InputFileName: %s, NReduce: %d, ReduceId: %d)",
		t.TaskType, t.TaskId, t.InputFileName, t.NReduce, t.ReduceId)
}

type GetTaskRequest struct {
}

type GetTaskReply struct {
	Task Task
}

type CompleteTaskRequest struct {
	TaskId int
}

type CompleteTaskReply struct {
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
