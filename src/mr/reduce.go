package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceWorker(args RequestMapTask, reply TaskReply, reducef func(string, []string) string) {
	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("reduceWorker %v cannot open %v", reply.reduceReply.WorkerId, reply.File)
		return
	}

	kva := jsonFromFile(file)
	sort.Sort(ByKey(kva))
	reduce("mr-out-"+reply.reduceReply.WorkerId, kva, reducef)
}

func jsonFromFile(f *os.File) []KeyValue {
	dec := json.NewDecoder(f)
	kva := make([]KeyValue, 0, 0)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func reduce(ofile string, kva []KeyValue, reducef func(string, []string) string) {
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0, 0)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		writeOutputFile(ofile, output, kva[i])

		i = j
	}
}

func writeOutputFile(ofile, output string, kva KeyValue) {
	file, err := os.Create(ofile)
	if err != nil {
		log.Fatalf("writeOutputFile os.Create failed")
		return
	}
	if _, err := fmt.Fprintf(file, "%v %v\n", kva.Key, output); err != nil {
		log.Fatalf("writeOutputFile Fprintf failed")
	}
}
