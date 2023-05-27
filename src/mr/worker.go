package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// generateWorkId by Similar Snowflake algorithm, tips(Thanks to ChatGPT!)
func generateWorkId() string {
	// Generate a timestamp (in milliseconds)
	timestamp := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	// Generate a random integer between 0 and 999
	randomInt := rand.Intn(1000)
	// Concatenate the timestamp and random integer
	uniqueID := timestamp + strconv.Itoa(randomInt)
	// Print the unique ID
	return uniqueID
}

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
	retry := 3
	for {
		workerId := generateWorkId()
		args := WorkArgs{
			WorkerId: workerId,
		}
		reply := WorkReply{}
		call("Coordinator.Work", &args, &reply)
		if reply.isDone {
			log.Println("All tasks are completed")
			return
		}
		switch reply.MapReduce {
		case "Map":
			MapWork(reply, mapf)
			retry = 3
		case "Reduce":
			ReducefWork(reply, reducef)
			retry = 3
		default:
			log.Println("error reply: would retry times:", retry)
			if retry < 0 {
				return
			}
			retry--
		}
		CallCommit(workerId, reply.TaskId, reply.MapReduce)
	}
}

func MapWork(reply WorkReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", file.Name())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file.Name())
	}
	file.Close()
	intermediate := mapf(file.Name(), string(content))

	sort.Sort(ByKey(intermediate))

	tmpFileName := "mr-tmp-" + strconv.Itoa(reply.TaskId)
	var fileBucket = make(map[int]*json.Encoder)
	for i := 0; i < reply.BucketNumber; i++ {
		ofile, _ := os.Create(tmpFileName + strconv.Itoa(i))
		defer ofile.Close()
		fileBucket[i] = json.NewEncoder(ofile)
	}
	for _, kv := range intermediate {
		bucketId := ihash(kv.Key) % reply.BucketNumber
		err := fileBucket[bucketId].Encode(&kv)
		if err != nil {
			log.Fatalf("unable to write to file")
		}
	}
}
func ReducefWork(WorkReply, func(string, []string) string) {
	return
}

func CallCommit(workerId string, TaskId int, MapReduce string) {
	args := CommitArgs{
		WorkerId:  workerId,
		TaskId:    TaskId,
		MapReduce: MapReduce,
	}
	reply := CommitReply{}
	call("Coordinator.Commit", &args, &reply)
	if reply.isOk {
		fmt.Printf("The submission was successful")
	}
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
