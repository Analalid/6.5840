package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"syscall"
)
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey是一个用于排序的包装类
type ByKey []KeyValue

// for sorting by key.
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

var mapf func(string, string) []KeyValue
var reducef func(string, []string) string

// main/mrworker.go calls this function.
func Worker(mapff func(string, string) []KeyValue,
	reduceff func(string, []string) string) {
	//插件注册
	mapf = mapff
	reducef = reduceff
	allMapFiles := CallGetMapTask()
	MergeAll(allMapFiles)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallGetMapTask() map[int][]*os.File {
	allMapsFiles := make(map[int][]*os.File)
	//线程同步
	var wg sync.WaitGroup
	////TODO 尚未完成容错功能
	for {
		args := GetMapTaskArgs{}
		reply := GetMapTaskReply{}
		ok := call("Coordinator.GetMapTaskFileName", &args, &reply)
		// 应当返回一个文件名, reply.MapTaskFileName
		if !ok {
			fmt.Println("获取任务失败，目前已经没有任务了")
			break
		}
		fmt.Printf("MapTaskFileName %v\n", reply.MapTaskFileName)
		task := reply.MapTaskFileName
		reduceNum := reply.ReduceNum
		wg.Add(1)
		//新开一个线程执行map任务
		go func(task string, reduceNum int) {
			defer wg.Done()
			mapTask := MapTask(task, reduceNum)
			for k, v := range mapTask {
				mutex := sync.Mutex{}
				mutex.Lock()
				allMapsFiles[k] = append(allMapsFiles[k], v)
				mutex.Unlock()
			}
		}(task, reduceNum)
	}
	wg.Wait()
	fmt.Printf("MAP任务全部完成")
	return allMapsFiles
}

func MergeAll(allMapFiles map[int][]*os.File) {
	for k, v := range allMapFiles {
		//创建文件mr-out-k
		oname := "mr-out-" + strconv.Itoa(k)
		fmt.Println("合并" + oname + "==================================")
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		for _, _file := range v {
			//解析.json文件
			file, _ := os.Open(_file.Name())
			enc := json.NewDecoder(file)
			for _, kv := range []struct{ Key, Value string }{
				{"foo", "bar"},
				{"hello", "world"},
			} {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("写入文件失败：", err)
					return
				}
			}
		}
	}
}

func MapTask(task string, reduceNum int) map[int]*os.File {
	var intermediate []KeyValue
	file, err := os.Open(task)
	if err != nil {
		log.Fatalf("cannot open %v", task)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task)
	}
	file.Close()
	kva := mapf(task, string(content))
	intermediate = append(intermediate, kva...)
	//排序
	sort.Sort(ByKey(intermediate))
	//新建文件
	tid := syscall.Gettid()
	oname := "mr-" + strconv.Itoa(tid) + "-"
	//创建files
	filenameMap := make(map[int]*os.File)
	for i := 0; i < reduceNum; i++ {
		//以.json结尾，方便解析
		createdFile, _ := os.Create(oname + strconv.Itoa(i) + ".json")
		filenameMap[i] = createdFile
		defer createdFile.Close()
	}
	//写入文件
	i := 0
	//得到线程ID
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
		//根据不同的key选择不同的文件写入, reduceNum 默认是10
		bucketId := ihash(intermediate[i].Key) % reduceNum
		//写入文件
		fmt.Fprintf(filenameMap[bucketId], "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return filenameMap
}
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
