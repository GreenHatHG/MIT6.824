package mr

import (
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState int

const (
	Idle TaskState = iota
	InProgress
)

type WorkerType int

const (
	Map WorkerType = iota
	Reduce
)

type Task struct {
	WorkerId string
	State    TaskState
	Files    []string
	//"m" or "r"
	WorkerType WorkerType
}

type Master struct {
	InputFiles []string
	//key: ReduceWorkerId
	IntermediateFiles sync.Map
	//key: WorkerId
	TaskList       sync.Map
	MapTaskChannel chan string
	//ReduceWorkerId
	ReduceTaskChannel chan string
	NReduce           int
	MU                *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) putTask() {
	for i := 0; i < m.NReduce; i++ {
		m.ReduceTaskChannel <- strconv.Itoa(i)
	}
	for _, file := range m.InputFiles {
		m.MapTaskChannel <- file
	}
}

func (m *Master) AssignTask(args *Args, reply *TaskReply) error {
	workerType := Map
	if len(m.MapTaskChannel) == 0 && m.checkTaskIdle(Map) {
		workerType = Reduce
	}
	reply.IsDone = m.isDone()
	if workerType == Map {
		mapFile := <-m.MapTaskChannel
		m.startMapTask(mapFile, reply)
	} else if workerType == Reduce {
		reduceWorkerId := <-m.ReduceTaskChannel
		if id, ok := m.IntermediateFiles.Load(reduceWorkerId); ok {
			m.startReduceTask(id.([]string), reply)
		}
	}
	return nil
}

func (m *Master) startMapTask(file string, reply *TaskReply) {
	reply.WorkerType = Map
	reply.WorkerId = m.getMapTaskWorkerId()
	m.commonSetting([]string{file}, reply)
}

func (m *Master) startReduceTask(files []string, reply *TaskReply) {
	reply.WorkerType = Reduce
	reply.WorkerId = m.getReduceWorkerId(files[0])
	m.commonSetting(files, reply)
}

func (m *Master) commonSetting(files []string, reply *TaskReply) {
	reply.NReduce = m.NReduce
	reply.Files = files
	m.TaskList.Store(reply.WorkerId, Task{
		WorkerId:   reply.WorkerId,
		State:      InProgress,
		Files:      files,
		WorkerType: reply.WorkerType,
	})
	go m.timeoutScheduledTask(reply.WorkerId)
}

func (m *Master) getMapTaskWorkerId() string {
	rand.Seed(time.Now().UnixNano())
	min := 100
	max := 200000
	return strconv.Itoa(rand.Intn(max-min) + min)
}

func (m *Master) MapTaskFinish(args *MapFinish, reply *TaskReply) error {
	for _, file := range args.IntermediateFiles {
		id := m.getReduceWorkerId(file)
		if arr, ok := m.IntermediateFiles.Load(id); ok {
			m.IntermediateFiles.Store(id, append(arr.([]string), file))
		} else {
			m.IntermediateFiles.Store(id, []string{file})
		}
	}
	m.initTaskState(args.WorkerId)
	return nil
}

func (m *Master) ReduceTaskFinish(args *ReduceFinish, reply *TaskReply) error {
	m.initTaskState(args.WorkerId)
	return nil
}

func (m *Master) getReduceWorkerId(intermediateFile string) string {
	//"mr-X-Y"
	arr := strings.Split(intermediateFile, "-")
	if len(arr) == 3 && arr[2] != "" {
		return arr[2]
	}
	return ""
}

func (m *Master) initTaskState(workerId string) {
	m.TaskList.Store(workerId, Task{
		State: Idle,
	})
}

func (m *Master) checkTaskIdle(workerType WorkerType) bool {
	//log.Println("checkTaskIdle", workerType)
	flag := true
	f := func(k, v interface{}) bool {
		task := v.(Task)
		if task.WorkerType == workerType && task.State != Idle {
			flag = false
			return false
		}
		return true
	}
	m.TaskList.Range(f)
	return flag
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
func (m *Master) isDone() bool {
	return m.checkTaskIdle(Map) && m.checkTaskIdle(Reduce) && len(m.MapTaskChannel) == 0 && len(m.ReduceTaskChannel) == 0
}

func (m *Master) Done() bool {
	done := m.isDone()
	//log.Println("done", done)
	return done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.TaskList = sync.Map{}
	m.IntermediateFiles = sync.Map{}
	m.MU = new(sync.RWMutex)
	m.NReduce = nReduce
	m.MapTaskChannel = make(chan string, 5)
	m.ReduceTaskChannel = make(chan string, nReduce)
	m.InputFiles = files
	go m.putTask()
	time.Sleep(time.Second * 1)

	m.server()
	return &m
}
