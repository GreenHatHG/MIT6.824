package mr

import (
	"errors"
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
var intermediateFileLock, taskLock sync.Mutex

func (m *Master) AssignTask(args *RequestMapTask, reply *TaskReply) error {
	taskLock.Lock()

	if len(m.InputFiles) > 0 {
		m.startMapTask(reply)
	} else if len(m.IntermediateFiles) > 0 {
		m.startReduceTask(reply)
	} else {
		m.isDone(reply)
	}

	defer taskLock.Unlock()
	return nil
}

func (m *Master) startMapTask(reply *TaskReply) {
	reply.File = m.InputFiles[0]
	reply.WorkerType = "m"
	reply.MapReply.NReduce = m.NReduce
	m.InputFiles = m.InputFiles[1:]
}

func (m *Master) startReduceTask(reply *TaskReply) {
	reply.File = m.IntermediateFiles[0]
	if workerId, err := getReduceWorkerId(reply.File); err == nil {
		reply.WorkerType = "r"
		reply.ReduceReply.WorkerId = workerId
	}
	m.IntermediateFiles = m.IntermediateFiles[1:]
}

func (m *Master) isDone(reply *TaskReply) {
	//tg:all worker done
	if len(m.InputFiles) == 0 && len(m.IntermediateFiles) == 0 {
		reply.IsDone = true
		m.IsDone = true
	}
}

func (m *Master) AddIntermediateFile(args *MapFinish, reply *TaskReply) error {
	intermediateFileLock.Lock()
	m.IntermediateFiles = append(m.IntermediateFiles, args.IntermediateFile)
	defer intermediateFileLock.Unlock()
	return nil
}

func getReduceWorkerId(intermediateFile string) (string, error) {
	//"mr-X-Y"
	arr := strings.Split(intermediateFile, "-")
	if len(arr) == 3 && arr[2] != "" {
		return arr[2], nil
	}
	return "", errors.New(`reduceWorkerId is ""`)
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
