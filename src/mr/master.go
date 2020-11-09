package mr

import (
	"fmt"
	"log"
	"strings"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	InputFiles        []string
	NReduce           int
	IntermediateFiles []string
	IsDone            bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

var intermediateFileLock, taskLock sync.Mutex

func (m *Master) AssignTask(args *RequestMapTask, reply *TaskReply) error {
	taskLock.Lock()
	if len(m.InputFiles) > 0 {
		m.startMapTask(args, reply)
	} else if len(m.IntermediateFiles) > 0 {
		m.startReduceTask(args, reply)
	}

	fmt.Println("len(m.InputFiles)", len(m.InputFiles))
	fmt.Println("len(m.IntermediateFiles)", len(m.IntermediateFiles))

	m.isDone(args, reply)
	taskLock.Unlock()
	return nil
}

func (m *Master) startMapTask(args *RequestMapTask, reply *TaskReply) {
	reply.File = m.InputFiles[0]
	reply.WorkerType = "m"
	reply.MapReply.NReduce = m.NReduce
	m.InputFiles = m.InputFiles[1:]
	fmt.Println("len(m.InputFiles)", len(m.InputFiles[1:]))
	fmt.Println("len(m.InputFiles)", len(m.InputFiles))
}

func (m *Master) startReduceTask(args *RequestMapTask, reply *TaskReply) {
	reply.File = m.IntermediateFiles[0]
	if workerId := getReduceWorkerId(reply.File); workerId != "" {
		reply.WorkerType = "r"
		reply.reduceReply.WorkerId = workerId
	}
	m.IntermediateFiles = m.IntermediateFiles[1:]
}

func (m *Master) isDone(args *RequestMapTask, reply *TaskReply) {
	//tg:all worker done
	if len(m.InputFiles) == 0 && len(m.IntermediateFiles) == 0 {
		reply.IsDone = true
		m.IsDone = true
	}
}

func (m *Master) AddIntermediateFile(args *MapFinish, reply *TaskReply) error {
	intermediateFileLock.Lock()
	m.InputFiles = append(m.InputFiles, args.IntermediateFile)
	intermediateFileLock.Unlock()
	return nil
}

func getReduceWorkerId(intermediateFile string) string {
	//"mr-X-Y"
	arr := strings.Split(intermediateFile, "-")
	if len(arr) == 3 {
		return arr[2]
	}
	return ""
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.IsDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.InputFiles = files
	m.NReduce = nReduce
	m.IntermediateFiles = make([]string, 0, 0)
	m.IsDone = false

	m.server()
	return &m
}
