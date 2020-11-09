package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}

// Add your RPC definitions here.
type MapArgs struct {
	WorkerId int
}

type MapReply struct {
	NReduce int
}

type ReduceReply struct {
	//master指定ReduceWorkerId
	//因为Map生成中间文件时已经带上ReduceWorkerId
	WorkerId string
}

type TaskReply struct {
	//"m" or "r"
	WorkerType  string
	File        string
	MapReply    MapReply
	reduceReply ReduceReply
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
