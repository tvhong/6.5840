package mr

import "fmt"
import "log"
// import "ioutil"
// import "os"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Operator struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (o *Operator) server() {
	CallGetTask()
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

func CallGetTask() {
	request := GetTaskRequest{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &request, &reply)
	if ok {
		fmt.Printf("Worker received Task: %s\n", reply.Task)
	} else {
		log.Fatalf("Call failed! Assuming coordinator exited. Exiting worker.")
	}

	task := reply.Task
	if task.TaskType == MapTask {

	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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

// func handleMap(task Task) {
// 	filename := task.InputFileName
// 	nReduce := task.NReduce
// 
// 	// Read the inputFile
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		log.Fatalf("cannot open %v", filename)
// 	}
// 	content, err := ioutil.ReadAll(file)
// 	if err != nil {
// 		log.Fatalf("cannot read %v", filename)
// 	}
// 	file.Close()
// 	kva := mapf(filename, string(content))
// 	intermediate = append(intermediate, kva...)
// 	// convert each line to key, value pair
// 	// hash the key to get reduceId
// 	// open the file for append and write to the file
// 	// get a lock (or create a coroutine for file writing) so writing to file doesn't conflict with another task
// 
// }
