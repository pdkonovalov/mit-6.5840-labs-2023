package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if ok {
			switch reply.TaskType {
			case 0:
				mapWork(mapf, &reply)
			case 1:
				reduceWork(reducef, &reply)
			case 2:
				time.Sleep(time.Second)
			case 3:
				return
			}
		} else {
			time.Sleep(time.Second)
		}
	}
}

func mapWork(mapf func(string, string) []KeyValue, reply *RequestTaskReply) {
	filename := reply.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	nReduce := reply.NReduce
	partitioned := make([][]KeyValue, nReduce, nReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		partitioned[i] = append(partitioned[i], kv)
	}
	outputFiles := make([]string, nReduce, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("tmp-%v-%v-%v", reply.Index, i, reply.CountOfSends)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v:", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range partitioned[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("error: ", err)
			}
		}
		outputFiles[i] = filename
		file.Close()
	}
	submitArgs := SubmitTaskArgs{0, outputFiles, reply.Index}
	submitReply := SubmitTaskReply{}
	call("Coordinator.SubmitTask", &submitArgs, &submitReply)
	return
}

func reduceWork(reducef func(string, []string) string, reply *RequestTaskReply) {
	intermediate := []KeyValue{}
	for _, filename := range reply.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	filename := fmt.Sprintf("tmp-%v-%v", reply.Index, reply.CountOfSends)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	file.Close()
	submitArgs := SubmitTaskArgs{1, []string{filename}, reply.Index}
	submitReply := SubmitTaskReply{}
	call("Coordinator.SubmitTask", &submitArgs, &submitReply)
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
