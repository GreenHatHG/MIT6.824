package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type Args struct {
}

type ReduceFinish struct {
	WorkerId string
}

type MapFinish struct {
	WorkerId          string
	IntermediateFiles []string
}

type TaskReply struct {
	//"m" or "r"
	WorkerType WorkerType
	WorkerId   string
	NReduce    int
	//mapTask only 1, reduceTask more
	Files  []string
	IsDone bool
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
