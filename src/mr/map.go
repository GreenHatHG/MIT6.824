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

func mapWorker(args MapArgs, reply TaskReply, mapf func(string, string) []KeyValue) {
	content, err := readFile(reply.File)
	if err != nil {
		log.Fatalf("worker %v cannot open %v", args.WorkerId, reply.File)
		return
	}
	kva := mapf(reply.File, content)
	writeIntermediateFile(args.WorkerId, reply.MapReply.NReduce, reply.File, kva)
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

func writeIntermediateFile(workerId, nReduce int, filename string, kva []KeyValue) {
	intermediateFile := fmt.Sprint("mr-", workerId, "-", ihash(filename)%nReduce)
	file, err := ioutil.TempFile(".", intermediateFile)
	if err != nil {
		log.Fatalf("worker %v cannot create file %v", workerId, intermediateFile)
		return
	}

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("worker %v cannot Encode %v", workerId, kv)
		}
	}

	if err := os.Rename(file.Name(), intermediateFile); err != nil {
		log.Fatalf("worker %v cannot Rename tempFile", workerId)
	}
}
