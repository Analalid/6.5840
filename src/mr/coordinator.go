package mr

import (
	"context"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	files        []string
	nReduce      int
	timeout      time.Duration
	isDone       bool
	mapTasks     []int
	mapTaskCount int
	reduceTasks  []int
	workerCommit map[string]int
	mux          sync.Mutex
}

const (
	TaskIdle = iota
	TaskWorking
	TaskCommit
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Work(args *WorkArgs, reply *WorkReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	for idx, file := range c.files {
		if c.mapTasks[idx] != TaskIdle {
			continue
		}
		//change TaskStatus to Working
		c.mapTasks[idx] = TaskWorking
		//set reply message
		reply.isDone = c.isDone
		reply.TaskId = idx
		reply.FileName = file
		reply.MapReduce = "Map"
		reply.BucketNumber = c.nReduce
		//detect out of time error
		timeout, _ := context.WithTimeout(context.Background(), c.timeout)
		go func() {
			select {
			case <-timeout.Done():
				{
					c.mux.Lock()
					defer c.mux.Unlock()
					if c.workerCommit[args.WorkerId] != TaskCommit && c.mapTasks[idx] != TaskCommit {
						//reset mapTasks Status to TaskIdle
						c.mapTasks[idx] = TaskIdle
					}
				}
			}
		}()
		return nil
	}
	//detect all Map Task commit
	if len(c.files) != c.mapTaskCount {
		return nil
	}
	for idx, v := range c.reduceTasks {
		if v != TaskIdle {
			continue
		}
		c.reduceTasks[idx] = TaskWorking
		reply.TaskId = idx
		reply.MapReduce = "Reduce"
		reply.BucketNumber = len(c.files)
		reply.isDone = c.isDone
		//detect out of time error
		timeout, _ := context.WithTimeout(context.Background(), c.timeout)
		go func() {
			select {
			case <-timeout.Done():
				{
					c.mux.Lock()
					defer c.mux.Unlock()
					if c.workerCommit[args.WorkerId] != TaskCommit && c.reduceTasks[idx] != TaskCommit {
						//reset mapTasks Status to TaskIdle
						c.reduceTasks[idx] = TaskIdle
					}
				}
			}
		}()
		return nil
	}
	//detect all Reduce Task commit
	for _, v := range c.reduceTasks {
		if v != TaskCommit {
			return nil
		}
	}
	c.isDone = true
	log.Printf("all map Task and reduce task finisked!")
	return nil
}
func (c *Coordinator) Commit(args *CommitArgs, reply *CommitReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	switch args.MapReduce {
	case "Map":
		c.workerCommit[args.WorkerId] = TaskCommit
		c.mapTasks[args.TaskId] = TaskCommit
		c.mapTaskCount++
	case "Reduce":
		c.workerCommit[args.WorkerId] = TaskCommit
		c.reduceTasks[args.TaskId] = TaskCommit
	default:
		log.Fatalf("Commit Parameter error")
	}
	reply.isOk = true
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
	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		mapTasks:     make([]int, len(files)),
		mapTaskCount: 0,
		reduceTasks:  make([]int, nReduce),
		workerCommit: make(map[string]int),
		isDone:       false,
		timeout:      10 * time.Second,
	}
	log.Println("[init] with:", files, nReduce)
	c.server()
	return &c
}
