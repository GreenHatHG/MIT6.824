package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type RequestMapTask struct {
	WorkerId int
}

type ReduceFinish struct {
	WorkerId string
}

type MapFinish struct {
	File              string
	IntermediateFiles []string
}

type MapReply struct {
	NReduce int
	File    string
}

type ReduceReply struct {
	//master指定ReduceWorkerId
	//因为Map生成中间文件时已经带上ReduceWorkerId
	WorkerId string
	Files    []string
}

type TaskReply struct {
	//"m" or "r"
	WorkerType  string
	MapReply    MapReply
	ReduceReply ReduceReply
	IsDone      bool
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
