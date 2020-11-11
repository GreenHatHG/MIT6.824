package mr

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	StartTime time.Time
	File      string
	//"m" or "r"
	Type string
}

type Master struct {
	// Your definitions here.
	InputFiles        *SafeList
	NReduce           int
	IntermediateFiles *SafeList
	IsDone            bool
	TaskList          *SafeList
}

// Your code here -- RPC handlers for the worker to call.
var assignTaskLock sync.Mutex
var wg sync.WaitGroup

func (m *Master) AssignTask(args *RequestMapTask, reply *TaskReply) error {
	assignTaskLock.Lock()

	if m.InputFiles.Len() > 0 {
		wg.Add(1)
		m.startMapTask(reply)
	} else if m.IntermediateFiles.Len() > 0 {
		wg.Wait()
		if m.InputFiles.Len() == 0 {
			m.startReduceTask(reply)
		}
	} else {
		m.isDone(reply)
	}

	assignTaskLock.Unlock()
	return nil
}

func (m *Master) checkCrash() {
	m.TaskList.MU.Lock()
	log.Println("checkcrash")
	for e := m.TaskList.Data.Front(); e != nil; {
		if time.Now().Sub(e.Value.(Task).StartTime).Seconds() > 10.0 {
			fmt.Println("checkcrash", e.Value.(Task))
			if e.Value.(Task).Type == "m" {
				wg.Done()
				m.InputFiles.MU.Lock()
				m.InputFiles.Add(e.Value.(Task).File)
				m.InputFiles.MU.Unlock()
			} else {
				m.IntermediateFiles.MU.Lock()
				m.IntermediateFiles.Add(e.Value.(Task).File)
				m.IntermediateFiles.MU.Unlock()
			}
			next := e.Next()
			m.TaskList.Remove(e.Value)
			e = next
		} else {
			e = e.Next()
		}
	}
	log.Println("checkcrash finish")
	m.TaskList.MU.Unlock()
}

func (m *Master) startMapTask(reply *TaskReply) {
	reply.MapReply.File = m.InputFiles.Data.Front().Value.(string)
	m.TaskList.Add(Task{
		StartTime: time.Now(),
		File:      reply.MapReply.File,
		Type:      "m",
	})
	reply.WorkerType = "m"
	reply.MapReply.NReduce = m.NReduce
	m.InputFiles.Remove(m.InputFiles.Data.Front().Value)
}

func (m *Master) startReduceTask(reply *TaskReply) {
	m.IntermediateFiles.MU.Lock()
	//获取一个reduceWorkId
	index, err := getReduceWorkerId(m.IntermediateFiles.Data.Front().Value.(string))
	if err != nil {
		m.IntermediateFiles.Remove(m.IntermediateFiles.Data.Front().Value)
		return
	}

	files := make([]string, 0, 0)
	//找出所有与index相同的file
	for e := m.IntermediateFiles.Data.Front(); e != nil; {
		i, err := getReduceWorkerId(e.Value.(string))
		if err != nil {
			next := e.Next()
			m.IntermediateFiles.Remove(e.Value)
			e = next
		} else if i == index {
			next := e.Next()
			files = append(files, e.Value.(string))
			m.IntermediateFiles.Remove(e.Value)
			e = next
		} else {
			e = e.Next()
		}
	}

	if len(files) > 0 {
		m.TaskList.Add(Task{
			StartTime: time.Now(),
			//workerId
			File: index,
			Type: "r",
		})
		reply.WorkerType = "r"
		reply.ReduceReply.WorkerId = index
		reply.ReduceReply.Files = files
	}
	m.IntermediateFiles.MU.Unlock()
}

func (m *Master) isDone(reply *TaskReply) {
	if m.InputFiles.Len() == 0 && m.IntermediateFiles.Len() == 0 {
		reply.IsDone = true
		m.IsDone = true
	}
}

func (m *Master) AddIntermediateFile(args *MapFinish, reply *TaskReply) error {
	for _, file := range args.IntermediateFiles {
		m.IntermediateFiles.Add(file)
	}
	m.TaskList.Remove("m", args.File)
	defer wg.Done()
	return nil
}

func (m *Master) ReduceFinish(args *ReduceFinish, reply *TaskReply) error {
	m.TaskList.Remove("r", args.WorkerId)
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
	time.Sleep(time.Second * 5)
	m.checkCrash()
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
	m.InputFiles = NewSafeList()
	for _, file := range files {
		m.InputFiles.Add(file)
	}
	m.NReduce = nReduce
	m.IntermediateFiles = NewSafeList()
	m.IsDone = false
	m.TaskList = NewSafeList()

	m.server()
	return &m
}
