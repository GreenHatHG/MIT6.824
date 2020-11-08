package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
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



type WorkerInstance struct{
	WorkerId int
}

var instance WorkerInstance
var once sync.Once

func getWorkerInstance() WorkerInstance {
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
		instance = WorkerInstance{WorkerId: rand.Intn(100000)}
	})
	return instance
}

func readFile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", nil
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", nil
	}
	_ = file.Close()
	return string(content), err
}

func writeIntermediateFile(workerId, nReduce int, filename string, kva []KeyValue){
	intermediateFile := fmt.Sprint("mr-", workerId, "-", ihash(filename)%nReduce)
	file, err := ioutil.TempFile(".", intermediateFile)
	if err != nil{
		log.Fatalf("worker %v cannot create file %v", workerId, intermediateFile)
		return
	}

	enc := json.NewEncoder(file)
	for _, kv := range kva{
		if err := enc.Encode(&kv); err != nil{
			log.Fatalf("worker %v cannot Encode %v", workerId, kv)
		}
	}

	if err := os.Rename(file.Name(), intermediateFile); err != nil{
		log.Fatalf("worker %v cannot Rename tempFile", workerId)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	args := WorkerArgs{WorkerId:getWorkerInstance().WorkerId}
	reply := TaskReply{}
	call("Master.AssignTask", &args, &reply)
	fmt.Println(args.WorkerId, reply.File)

	content, err := readFile(reply.File)
	if err != nil{
		log.Fatalf("worker %v cannot open %v", args.WorkerId, reply.File)
		return
	}
	kva := mapf(reply.File, content)
	writeIntermediateFile(args.WorkerId, reply.NReduce, reply.File, kva)


}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}

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
