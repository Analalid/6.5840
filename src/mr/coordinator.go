package mr

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	//未完成的文件的任务列表
	files *list.List
	//针对得到Map任务的互斥锁
	MutexOfGetMapTaskFile sync.Mutex
	//nReduce = 10
	nReduce   int
	mapTaskId int
	mapTask   map[string]string
}

// Your code here -- RPC handlers for the worker to call.

//an example RPC handler.
//
//the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) GetMapTaskFileName(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.MutexOfGetMapTaskFile.Lock()
	defer c.MutexOfGetMapTaskFile.Unlock()
	if c.files.Len() == 0 {
		return errors.New("任务已经被取完了")
	} else {
		var res = c.files.Front()
		value, ok := res.Value.(string)
		reply.MapTaskFileName = value
		reply.ReduceNum = c.nReduce
		if ok {
			fmt.Println(value)
		} else {
			fmt.Println("Value is not a string")
		}
		c.files.Remove(res)
		return nil
	}
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

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//TODO
	log.Println("MakeCoordinator启动！==========")
	c := Coordinator{
		files: list.New(),
	}
	for _, value := range files {
		c.MutexOfGetMapTaskFile.Lock()
		c.files.PushBack(value)
		c.MutexOfGetMapTaskFile.Unlock()
	}
	c.nReduce = nReduce
	fmt.Printf("================nReduce: %d", nReduce)
	c.server()
	return &c
}

//// filenameRegister is a deprecated function.废弃
//// Deprecated: This function is deprecated. Use NewFunc instead.
//func (c Coordinator) filenameRegister() {
//	dir, err := os.Getwd()
//	dir = dir + "/main"
//	files, err := ioutil.ReadDir(dir)
//	//fmt.Println("查询的文件夹是 " + dir)
//	if err != nil {
//		fmt.Println("Error:", err)
//		return
//	}
//	//匹配pg开头，txt结尾
//	pattern := "^pg.*txt$"
//	regex, err := regexp.Compile(pattern)
//	if err != nil {
//		fmt.Println("Error:", err)
//		return
//	}
//	for _, file := range files {
//		if !file.IsDir() && regex.MatchString(file.Name()) {
//			c.files[file.Name()] = true
//			//输出文件
//			//fmt.Println(filepath.Join(dir, file.Name()))
//		}
//	}
//}
