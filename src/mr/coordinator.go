package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type State string
const (
	STATE_MAP State = "state_map"
	STATE_REDUCE = "state_reduce"
	STATE_DONE = "state_done"
)

type Coordinator struct {
	todoTasks []Task
	inprogressTasks map[int]Task
	completedTasks []Task
	state State
}

func (c *Coordinator) GetTask(request *GetTaskRequest, reply *GetTaskReply) error {
	var task Task

	if c.state == STATE_DONE {
		task = Task{}
		task.TaskType = ExitTask
		reply.Task = task
		return nil
	}

	if len(c.todoTasks) == 0 {
		if c.state != STATE_REDUCE {
			log.Fatalf("When there is no todoTasks, the state should be REDUCE, or DONE. Instead, it is %s", c.state)
		}
		task = Task{}
		task.TaskType = WaitTask
		reply.Task = task
		return nil
	}

	if c.todoTasks[0].TaskType == ReduceTask && c.state != STATE_REDUCE {
		task = Task{}
		task.TaskType = WaitTask
		reply.Task = task
		return nil
	}

	task = c.todoTasks[0]
	reply.Task = task
	c.todoTasks = c.todoTasks[1:]
	c.inprogressTasks[task.TaskId] = task

	return nil
}

func (c *Coordinator) CompleteTask(request *CompleteTaskRequest, reply *CompleteTaskReply) error {
	// Remove task from inprogressTasks and add it to completedTasks
	// Check if no more map task in todoTasks or inprogressTasks, set the state from MAP to REDUCE
	// Check if no tasks in todoTasks and no tasks inprogressTasks, then the state from REDUCE to DONE
	printf("[Coordinator] Worker completed task %d\n", request.TaskId)
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
	c.inprogressTasks = make(map[int]Task)
	c.completedTasks = make([]Task, 0)
	c.state = STATE_MAP
	c.server()
	return &c
}

func (c *Coordinator) initTodos(files []string, nReduce int) {
	c.todoTasks = make([]Task, 0)

	taskId := 0
	for _, file:= range(files) {
		task := Task{}
		task.TaskType = MapTask
		task.TaskId = taskId
		task.InputFileName = file
		task.NReduce = nReduce
		c.todoTasks = append(c.todoTasks, task)

		taskId += 1
	}

	for i := 0; i < nReduce; i++ {
		task := Task{}
		task.TaskType = ReduceTask
		task.TaskId = taskId
		task.ReduceId = i
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
