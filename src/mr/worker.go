package mr

import "fmt"
import "io"
import "log"
import "os"
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
}

func (o *Operator) server() {
	task := o.getTask()
	o.handleTask(task)
}

func (o *Operator) getTask() Task {
	request := GetTaskRequest{}
	reply := GetTaskReply{}

	ok := o.call("Coordinator.GetTask", &request, &reply)
	if ok {
		fmt.Printf("Worker received Task: %s\n", reply.Task)
		return reply.Task
	} else {
		fmt.Printf("Call failed! Assuming coordinator exited. Exiting worker.")

		task := Task{}
		task.TaskType = ExitTask
		return task
	}
}

func (o *Operator) handleTask(task Task) {
	if task.TaskType == MapTask {
		o.handleMap(task)
	}
}

func (o *Operator) handleMap(task Task) {
	filename := task.InputFileName
	nReduce := task.NReduce

	// Read the inputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := o.mapf(filename, string(content))
	o.writeInterFiles(kva, nReduce)
}

func (o *Operator) writeInterFiles(kva []KeyValue, nReduce int) {
	files := make(map[string]*os.File)
	for i := 0; i < nReduce; i++ {
		filename := o.getInterFilename(i)
		// TODO: use append, use lock (or coroutine for file writing)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}

		file.WriteString("Hello\n")
		fmt.Fprintln(file, "World")

		files[filename] = file

		defer file.Close()
	}

	for _, kv := range(kva) {
		filename := o.getInterFilename(o.getReduceId(kv, nReduce))
		file := files[filename]
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}
}

func (o *Operator) getReduceId(kv KeyValue, nReduce int) int {
	return o.ihash(kv.Key) % nReduce
}

func (o *Operator) getInterFilename(reduceId int) string {
	return fmt.Sprintf("mr-inter-%d", reduceId)
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
