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
	kva := make([]KeyValue, 0, 0)
	for _, file := range reply.ReduceReply.Files {
		kva = append(kva, jsonFromFile(file)...)
	}
	sort.Sort(ByKey(kva))
	reduce("mr-out-"+reply.ReduceReply.WorkerId, kva, reducef)
}

func jsonFromFile(filename string) []KeyValue {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
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
	file, err := os.Create(ofile)
	if err != nil {
		log.Fatalf("writeOutputFile os.Create failed")
		return
	}

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
		if _, err := fmt.Fprintf(file, "%v %v\n", kva[i].Key, output); err != nil {
			log.Fatalf("writeOutputFile Fprintf failed")
		}

		i = j
	}
}
