package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

type MapWorkerInstance struct {
	WorkerId int
}

var instance MapWorkerInstance
var once sync.Once

func getMapWorkerInstance() MapWorkerInstance {
	once.Do(func() {
		rand.Seed(time.Now().UnixNano())
		instance = MapWorkerInstance{WorkerId: rand.Intn(100000)}
	})
	return instance
}

func mapWorker(args RequestMapTask, reply TaskReply, mapf func(string, string) []KeyValue) []string {
	content, err := readFile(reply.MapReply.File)
	if err != nil {
		log.Fatalf("worker %v cannot open %v", args.WorkerId, reply.MapReply.File)
		return []string{}
	}
	kva := mapf(reply.MapReply.File, content)
	return writeIntermediateFile(args, reply, kva)
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

func writeIntermediateFile(args RequestMapTask, reply TaskReply, kva []KeyValue) []string {
	shufflingData := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceWorkerId := ihash(kv.Key) % reply.MapReply.NReduce
		shufflingData[reduceWorkerId] = append(shufflingData[reduceWorkerId], kv)
	}

	intermediateFiles := make([]string, 0, 0)
	for index, kva := range shufflingData {
		intermediateFile := fmt.Sprint("mr-", args.WorkerId, "-", index)
		intermediateFiles = append(intermediateFiles, intermediateFile)
		tempFileAndRename(intermediateFile, kva)
	}
	return intermediateFiles
}

func tempFileAndRename(intermediateFile string, kva []KeyValue) {
	file, err := ioutil.TempFile(".", intermediateFile)
	if err != nil {
		log.Fatalf("cannot create file")
		return
	}
	enc := json.NewEncoder(file)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("cannot Encode")
		}
	}

	if err := os.Rename(file.Name(), intermediateFile); err != nil {
		log.Fatalf("cannot Rename tempFile")
	}
}
