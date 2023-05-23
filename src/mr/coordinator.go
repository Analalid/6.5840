package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	//nReduce 每当我查询大一个文件代表多了一个任务
	files   map[string]bool
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

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

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	filenameRegister()
	// Your code here.

	c.server()
	return &c
}

func (c Coordinator) filenameRegister() {
	dir, err := os.Getwd()
	dir = dir + "/main"
	files, err := ioutil.ReadDir(dir)
	//fmt.Println("查询的文件夹是 " + dir)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	//匹配pg开头，txt结尾
	pattern := "^pg.*txt$"
	regex, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	for _, file := range files {
		if !file.IsDir() && regex.MatchString(file.Name()) {
			c.files[file.Name()] = true
			//输出文件
			//fmt.Println(filepath.Join(dir, file.Name()))
		}
	}
}
