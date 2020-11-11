package mr

import (
	"fmt"
	"math/rand"
	"time"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := RequestMapTask{WorkerId: getWorkerId()}
	reply := TaskReply{}
	call("Master.AssignTask", &args, &reply)
	fmt.Printf("%+v\n", reply)
	if reply.IsDone {
		return
	}

	if reply.WorkerType == "m" {
		intermediateFiles := mapWorker(args, reply, mapf)
		call("Master.AddIntermediateFile", &MapFinish{
			IntermediateFiles: intermediateFiles,
			File:              reply.MapReply.File}, &TaskReply{})
	} else if reply.WorkerType == "r" {
		reduceWorker(args, reply, reducef)
		call("Master.ReduceFinish", &ReduceFinish{WorkerId: reply.ReduceReply.WorkerId}, &TaskReply{})
	}
	Worker(mapf, reducef)
}

func getWorkerId() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(999999)
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
