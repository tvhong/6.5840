package mr

import "errors"
import "fmt"
import "io"
import "log"
import "os"
import "regexp"
import "strings"
import "time"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Operator struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
	done bool
}

func (o *Operator) server() {
	for !o.done {
		task, err := o.callGetTask()
		if err == nil {
			o.handleTask(task)
		}
	}
}

func (o *Operator) callGetTask() (Task, error) {
	request := GetTaskRequest{}
	reply := GetTaskReply{}

	ok := o.call("Coordinator.GetTask", &request, &reply)
	if ok {
		printf("[Worker] Received Task: %s\n", reply.Task)
		return reply.Task, nil
	} else {
		printf("[Worker] Call failed! Assuming coordinator exited. Exiting worker.")
		o.done = true

		return Task{}, errors.New("Failed getting tasks")
	}
}

func (o *Operator) callCompleteTask(taskId int) bool {
	request := CompleteTaskRequest{}
	reply := CompleteTaskReply{}

	request.TaskId = taskId

	ok := o.call("Coordinator.CompleteTask", &request, &reply)
	if ok {
		printf("[Worker] Completed task %d\n", taskId)
		return true
	} else {
		printf("[Worker] Failed to send complete task! Assuming coordinator exited. Exiting worker.")
		o.done = true
		return false
	}
}

func (o *Operator) handleTask(task Task) {
	if task.TaskType == MapTask {
		o.handleMap(task)
	} else if task.TaskType == ReduceTask {
		o.handleReduce(task)
	} else if task.TaskType == WaitTask {
		o.handleWait(task)
	} else if task.TaskType == ExitTask {
		o.handleExit(task)
	}
}

func (o *Operator) handleMap(task Task) {
	filename := task.InputFileName
	nReduce := task.NReduce

	content := o.readFile(filename)

	kva := o.mapf(filename, content)
	o.writeMapResults(kva, nReduce)

	o.callCompleteTask(task.TaskId)
}

func (o *Operator) handleReduce(task Task) {
	reduceId := task.ReduceId
	filename := o.getInterFilename(reduceId)
	content := o.readFile(filename)
	lines := strings.Split(content, "\n")
	printf("[Worker] len(lines): %d\n", len(lines))

	// merge the values with same key together (map[string][]string)
	keyToValues := make(map[string][]string)
	for _, line := range(lines) {
		match, _ := regexp.MatchString(".* .*", line)
		if !match {
			continue
		}

		pair := strings.SplitN(line, " ", 2)
		kv := KeyValue{}
		kv.Key, kv.Value = pair[0], pair[1]

		_, ok := keyToValues[kv.Key]
		if !ok {
			keyToValues[kv.Key] = make([]string, 0)
		}

		keyToValues[kv.Key] = append(keyToValues[kv.Key], kv.Value)
	}

	kva := make([]KeyValue, 0)
	for key, values := range(keyToValues) {
		reducedValue := o.reducef(key, values)
		kva = append(kva, KeyValue{key, reducedValue})
	}

	o.writeReduceResults(kva)

	o.callCompleteTask(task.TaskId)
}

func (o *Operator) handleWait(task Task) {
	time.Sleep(time.Second)
}

func (o *Operator) handleExit(task Task) {
	o.done = true
}

func (o *Operator) readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func (o *Operator) writeMapResults(kva []KeyValue, nReduce int) {
	files := make(map[string]*os.File)
	for i := 0; i < nReduce; i++ {
		filename := o.getInterFilename(i)
		file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY | os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}

		files[filename] = file

		defer file.Close()
	}

	for _, kv := range(kva) {
		filename := o.getInterFilename(o.getReduceId(kv, nReduce))
		file := files[filename]
		// TODO: might need special separator
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
}

func (o *Operator) writeReduceResults(kva []KeyValue) {
	filename := o.getOutputFilename()
	file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY | os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	for _, kv := range(kva) {
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
}

func (o *Operator) getReduceId(kv KeyValue, nReduce int) int {
	return o.ihash(kv.Key) % nReduce
}

func (o *Operator) getInterFilename(reduceId int) string {
	return fmt.Sprintf("mr-inter-%d", reduceId)
}

func (o *Operator) getOutputFilename() string {
	return "mr-out-0"
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func (o *Operator) ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (o *Operator) call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func makeOperator(
		mapf func(string, string) []KeyValue,
		reducef func(string, []string) string) *Operator {
	o := Operator{}
	o.mapf = mapf
	o.reducef = reducef
	o.done = false

	return &o
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
		reducef func(string, []string) string) {
	operator := makeOperator(mapf, reducef)
	operator.server()
}
