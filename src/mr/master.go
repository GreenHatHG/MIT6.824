package mr

import (
	"container/list"
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
	IntermediateFiles *list.List
	IsDone            bool
}

// Your code here -- RPC handlers for the worker to call.
var intermediateFileLock, taskLock sync.Mutex

func (m *Master) AssignTask(args *RequestMapTask, reply *TaskReply) error {
	taskLock.Lock()

	if len(m.InputFiles) > 0 {
		m.startMapTask(reply)
	} else if m.IntermediateFiles.Len() > 0 {
		m.startReduceTask(reply)
	} else {
		m.isDone(reply)
	}

	defer taskLock.Unlock()
	return nil
}

func (m *Master) startMapTask(reply *TaskReply) {
	reply.MapReply.File = m.InputFiles[0]
	reply.WorkerType = "m"
	reply.MapReply.NReduce = m.NReduce
	m.InputFiles = m.InputFiles[1:]
}

func (m *Master) startReduceTask(reply *TaskReply) {
	//获取一个reduceWorkId
	index, err := getReduceWorkerId(m.IntermediateFiles.Front().Value.(string))
	if err != nil {
		m.IntermediateFiles.Remove(m.IntermediateFiles.Front())
		return
	}

	files := make([]string, 0, 0)
	//找出所有与index相同的file
	for e := m.IntermediateFiles.Front(); e != nil; e = e.Next() {
		if i, err := getReduceWorkerId(e.Value.(string)); err != nil {
			m.IntermediateFiles.Remove(e)
		} else if i == index {
			files = append(files, e.Value.(string))
			m.IntermediateFiles.Remove(e)
		}
	}

	if len(files) > 0 {
		reply.WorkerType = "r"
		reply.ReduceReply.WorkerId = index
		reply.ReduceReply.Files = files
	}
}

func (m *Master) isDone(reply *TaskReply) {
	//tg:all worker done
	if len(m.InputFiles) == 0 && m.IntermediateFiles.Len() == 0 {
		reply.IsDone = true
		m.IsDone = true
	}
}

func (m *Master) AddIntermediateFile(args *MapFinish, reply *TaskReply) error {
	intermediateFileLock.Lock()
	for _, file := range args.IntermediateFiles {
		m.IntermediateFiles.PushBack(file)
	}
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
	m.IntermediateFiles = list.New()
	m.IsDone = false

	m.server()
	return &m
}
