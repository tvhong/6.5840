package mr

import "log"
import "fmt"
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
		printLn("[Coordinator] GetTask returns ExitTask")
		return nil
	}

	if len(c.todoTasks) == 0 {
		if c.state != STATE_REDUCE {
			log.Fatalf("When there is no todoTasks, the state should be REDUCE, or DONE. Instead, it is %s", c.state)
		}
		task = Task{}
		task.TaskType = WaitTask
		reply.Task = task
		printLn("[Coordinator] GetTask returns WaitTask until all ReduceTasks complete")
		return nil
	}

	if c.todoTasks[0].TaskType == ReduceTask && c.state != STATE_REDUCE {
		task = Task{}
		task.TaskType = WaitTask
		reply.Task = task
		printLn("[Coordinator] GetTask returns WaitTask until all MapTasks complete")
		return nil
	}

	task = c.todoTasks[0]
	reply.Task = task
	c.todoTasks = c.todoTasks[1:]
	c.inprogressTasks[task.TaskId] = task

	printf("[Coordinator] GetTask returns %s", task.TaskType)

	return nil
}

func (c *Coordinator) CompleteTask(request *CompleteTaskRequest, reply *CompleteTaskReply) error {
	taskId := request.TaskId

	printf("[Coordinator] Worker completed task %d\n", taskId)

	task := c.inprogressTasks[taskId]
	delete(c.inprogressTasks, taskId)
	c.completedTasks = append(c.completedTasks, task)

	if (c.state == STATE_MAP && !c.hasTodoOfType(MapTask) && !c.hasInProgressOfType(MapTask)) {
		printLn("[Coordinator] Transition from STATE_MAP to STATE_REDUCE")
		c.state = STATE_REDUCE
	}

	if (c.state == STATE_REDUCE && len(c.todoTasks) == 0 && len(c.inprogressTasks) == 0) {
		printLn("[Coordinator] Transition from STATE_REDUCE to STATE_DONE")
		c.state = STATE_DONE
	}

	return nil
}

func (c *Coordinator) hasTodoOfType(taskType TaskType) bool {
	for _, task := range(c.todoTasks) {
		if task.TaskType == taskType {
			return true
		}
	}

	return false
}

func (c *Coordinator) hasInProgressOfType(taskType TaskType) bool {
	for _, task := range(c.inprogressTasks) {
		if task.TaskType == taskType {
			return true
		}
	}

	return false
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.state == STATE_DONE
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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	cleanFiles(nReduce)

	c := Coordinator{}
	c.initTodos(files, nReduce)
	c.inprogressTasks = make(map[int]Task)
	c.completedTasks = make([]Task, 0)
	c.state = STATE_MAP
	c.server()
	return &c
}

func cleanFiles(nReduce int) {
	os.Remove("mr-out-0")
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-inter-%d", i)
		os.Remove(filename)
	}
}
