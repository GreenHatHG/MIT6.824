package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type MapWorkerInstance struct {
	WorkerId int
}

func mapWorker(reply *TaskReply, mapf func(string, string) []KeyValue) []string {
	content, err := readFile(reply.Files[0])
	if err != nil {
		log.Fatalf("mapWorker:readFile error %v", err.Error())
	}
	kva := mapf(reply.Files[0], content)
	return writeIntermediateFile(reply, kva)
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

func writeIntermediateFile(reply *TaskReply, kva []KeyValue) []string {
	shufflingData := make(map[int][]KeyValue)
	for _, kv := range kva {
		reduceWorkerId := ihash(kv.Key) % reply.NReduce
		shufflingData[reduceWorkerId] = append(shufflingData[reduceWorkerId], kv)
	}

	intermediateFiles := make([]string, 0, 0)
	for index, kva := range shufflingData {
		intermediateFile := fmt.Sprint("mr-", reply.WorkerId, "-", index)
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
