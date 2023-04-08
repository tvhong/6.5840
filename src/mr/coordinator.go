package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type State int
const (
	STATE_MAP State = iota
	STATE_REDUCE
	STATE_SHUTDOWN
	STATE_DONE
)

type Coordinator struct {
	todoTasks []Task
	inprogressTasks map[int]Task
	completedTasks []Task
	state State
}

func (c *Coordinator) GetTask(request *GetTaskRequest, reply *GetTaskReply) error {
	// Returns the first available task
	// Unless we are waiting for all map tasks to complete
	// Remove task from todoTasks and add it to inprogressTasks

	return nil
}

func (c *Coordinator) CompleteTask(request *CompleteTaskRequest, reply *CompleteTaskReply) error {
	//reply.Y = args.X + 1
	// Remove task from inprogressTasks and add it to completedTasks
	// Check if no more map task in todoTasks or inprogressTasks, set the state from MAP to REDUCE
	// Check if no tasks in todoTasks and no tasks inprogressTasks, then the state from REDUCE to DONE
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// TODO: wait 10s for workers to ask for done task
	return c.state == STATE_DONE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.initTodos(files, nReduce)
	c.allMapTasksCompleted = false
	c.server()
	return &c
}

func (c *Coordinator) initTodos(files []string, nReduce int) {
	taskId := 0
	for _, file:= range(files) {
		task := Task{}
		task.taskType = MapTask
		task.taskId = taskId
		task.inputFileName = file
		task.nReduce = nReduce
		c.todoTasks = append(c.todoTasks, task)

		taskId += 1
	}

	for i := 0; i < nReduce; i++ {
		task := Task{}
		task.taskType = ReduceTask
		task.taskId = taskId
		task.reduceId = i
		c.todoTasks = append(c.todoTasks, task)

		taskId += 1
	}

	printLn(len(c.todoTasks))
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
