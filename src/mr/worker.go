package mr

import (
	"fmt"
)
import "log"
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

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	reply := &TaskReply{}
	call("Master.AssignTask", &Args{}, reply)
	fmt.Printf("%+v\n", reply)
	if reply.IsDone {
		return
	}

	if reply.WorkerType == Map {
		StartMapTask(reply, mapf)
	} else if reply.WorkerType == Reduce {
		StartReduceTask(reply, reducef)
	}
	Worker(mapf, reducef)
}

func StartMapTask(reply *TaskReply, mapf func(string, string) []KeyValue) {
	if len(reply.Files) == 0 {
		return
	}
	intermediateFiles := mapWorker(reply, mapf)
	call("Master.MapTaskFinish", &MapFinish{
		WorkerId:          reply.WorkerId,
		IntermediateFiles: intermediateFiles,
	}, &TaskReply{})
}

func StartReduceTask(reply *TaskReply, reducef func(string, []string) string) {
	if len(reply.Files) == 0 {
		return
	}
	reduceWorker(reply, reducef)
	call("Master.ReduceTaskFinish", &ReduceFinish{WorkerId: reply.WorkerId}, &TaskReply{})
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
