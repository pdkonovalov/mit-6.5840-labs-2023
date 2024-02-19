package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	waitResultTimeout = 10 * time.Second
)

type taskState struct {
	index          int
	countOfSends   int
	timeOfLastSend time.Time
}

type Coordinator struct {
	mu                sync.Mutex
	inputFiles        []string
	intermediateFiles [][]string
	outputFiles       []string
	mapQueue          []taskState
	mapDone           []bool
	mapDoneCount      int
	reduceQueue       []taskState
	reduceDone        []bool
	reduceDoneCount   int
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	if c.mapDoneCount != len(c.inputFiles) {
		for i, v := range c.mapQueue {
			if !c.mapDone[v.index] {
				c.mapQueue = c.mapQueue[i:]
				break
			}
		}
		if time.Since(c.mapQueue[0].timeOfLastSend) < waitResultTimeout {
			reply.TaskType = waitTask
		} else {
			reply.TaskType = mapTask
			reply.InputFiles = make([]string, 1, 1)
			reply.InputFiles[0] = c.inputFiles[c.mapQueue[0].index]
			reply.NReduce = len(c.intermediateFiles)
			reply.Index = c.mapQueue[0].index
			reply.CountOfSends = c.mapQueue[0].countOfSends
			c.mapQueue[0].countOfSends++
			c.mapQueue[0].timeOfLastSend = time.Now()
			c.mapQueue = append(c.mapQueue[1:], c.mapQueue[0])
		}
	} else if c.reduceDoneCount != len(c.intermediateFiles) {
		for i, v := range c.reduceQueue {
			if !c.reduceDone[v.index] {
				c.reduceQueue = c.reduceQueue[i:]
				break
			}
		}
		if time.Since(c.reduceQueue[0].timeOfLastSend) < waitResultTimeout {
			reply.TaskType = waitTask
		} else {
			reply.TaskType = reduceTask
			reply.InputFiles = c.intermediateFiles[c.reduceQueue[0].index]
			reply.NReduce = len(c.intermediateFiles)
			reply.Index = c.reduceQueue[0].index
			reply.CountOfSends = c.reduceQueue[0].countOfSends
			c.reduceQueue[0].countOfSends++
			c.reduceQueue[0].timeOfLastSend = time.Now()
			c.reduceQueue = append(c.reduceQueue[1:], c.reduceQueue[0])
		}
	} else {
		reply.TaskType = exitTask
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mu.Lock()
	if args.TaskType == mapTask && !c.mapDone[args.Index] {
		for i, file := range args.OutputFiles {
			fileFmt := fmt.Sprintf("mr-%v-%v", args.Index, i)
			c.intermediateFiles[i][args.Index] = fileFmt
			err := os.Rename(file, fileFmt)
			if err != nil {
				log.Fatalf("cannot rename %v to %v", file, fileFmt)
			}
		}
		c.mapDone[args.Index] = true
		c.mapDoneCount++
	} else if args.TaskType == reduceTask && !c.reduceDone[args.Index] {
		file := args.OutputFiles[0]
		fileFmt := fmt.Sprintf("mr-out-%v", args.Index)
		c.outputFiles[args.Index] = fileFmt
		err := os.Rename(file, fileFmt)
		if err != nil {
			log.Fatalf("cannot rename %v to %v", file, fileFmt)
		}
		c.reduceDone[args.Index] = true
		c.reduceDoneCount++
	}
	c.mu.Unlock()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	ret = c.reduceDoneCount == len(c.intermediateFiles)
	c.mu.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:        files,
		intermediateFiles: make([][]string, nReduce, nReduce),
		outputFiles:       make([]string, nReduce, nReduce),
		mapQueue:          make([]taskState, len(files)),
		mapDone:           make([]bool, len(files), len(files)),
		reduceQueue:       make([]taskState, nReduce),
		reduceDone:        make([]bool, nReduce, nReduce),
	}
	for i := range c.intermediateFiles {
		c.intermediateFiles[i] = make([]string, len(files), len(files))
	}
	for i := range c.mapQueue {
		c.mapQueue[i].index = i
	}
	for i := range c.reduceQueue {
		c.reduceQueue[i].index = i
	}
	c.server()
	return &c
}
