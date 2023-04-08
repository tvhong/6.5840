package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType string
const (
    MapTask TaskType = "map"
    ReduceTask = "reduce"
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

type Coordinator struct {
	// Your definitions here.
	pendingTasks []Task
	completedTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	taskId := 0
	for _, file:= range(files) {
		task := Task{}
		task.taskType = MapTask
		task.taskId = taskId
		task.inputFileName = file
		task.nReduce = nReduce
		c.pendingTasks = append(c.pendingTasks, task)

		taskId += 1
	}

	for i := 0; i < nReduce; i++ {
		task := Task{}
		task.taskType = ReduceTask
		task.taskId = taskId
		task.reduceId = i
		c.pendingTasks = append(c.pendingTasks, task)

		taskId += 1
	}

	fmt.Println(len(c.pendingTasks))

	c.server()
	return &c
}
