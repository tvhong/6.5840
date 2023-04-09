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
	taskType TaskType
	taskId int

	// Fields for MapTask
	inputFileName string
	nReduce int

	// Fields for ReduceTask
	reduceId int
}

func (t Task) String() string {
	return fmt.Sprintf("taskType: %s, taskId: %d, inputFileName: %s, nReduce: %d, reduceId: %d",
		t.taskType, t.taskId, t.inputFileName, t.nReduce, t.reduceId)
}

type GetTaskRequest struct {
}

type GetTaskReply struct {
	Task Task
}

type CompleteTaskRequest struct {
	taskId int
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
