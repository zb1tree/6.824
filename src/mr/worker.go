package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 定义task，method表示采取的方法，0:map 1:redduce
// 定义event 0:map任务完成 1:reduce任务完成
const MAP = 0
const REDUCE = 1
const DONE = 2
const MapData = "Coordinator.GetMapData"
const ReduceData = "Coordinator.GetReduceData"
const JoinWorker = "Coordinator.JoinWorker"

// Reduce功能获取的kv对
type RKeyValue struct {
	Key   string
	Value []string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
//
// for sorting by key.采用已有定义
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply, status := CallJoin()
	//fmt.Printf("%v %v", reply, status)
	var filename string
	task := task{}
	for status {
		//获取任务为map
		task = reply.Task
		if task.Method == MAP {
			//
			// read each input file,
			// pass it to Map,
			// accumulate the intermediate Map output.
			//
			intermediate := []KeyValue{}
			filename = task.Obj.(string)
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
			intermediate = append(intermediate, kva...)
			//传递map结果
			MapArgs := RPCArgs{}
			MapArgs.Kvdata = intermediate
			MapArgs.Index = filename
			reply = RPCReply{}
			ok := MapTransmit(&MapArgs, &reply)
			if ok {
				//fmt.Printf("map data %s transmitted\n", reply.Task.Obj)
			} else {
				//fmt.Printf("map data %s transmit failed\n", reply.Task.Obj)
			}
			//执行reduce任务
		} else if task.Method == REDUCE {
			data := task.Obj.(RKeyValue)
			result := reducef(data.Key, data.Value)
			ReduceArgs := RPCArgs{}
			ReduceArgs.Kvdata = append(ReduceArgs.Kvdata, KeyValue{Key: data.Key, Value: result})
			ReduceArgs.Index = strconv.Itoa(ihash(data.Key))
			reply = RPCReply{}
			ok := ReduceTransmit(&ReduceArgs, &reply)
			if ok {
				//fmt.Printf("reduce data %s transmitted\n", data.Key)
			} else {
				//fmt.Printf("map data %s transmit failed\n", data.Key)
			}
		} else if task.Method == DONE {
			status = false
			//fmt.Printf("task over!\n")
			return
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// 向协调进程发送一个信号表示新的worker进程加入，从返回值获取分配的任务
func CallJoin() (RPCReply, bool) {
	args := RPCArgs{}
	reply := RPCReply{}
	ok := call(JoinWorker, &args, &reply)
	if ok {
		//fmt.Printf("worker joined\n")
		return reply, true
	} else {
		//fmt.Printf("worker join failed!\n")
		return reply, false
	}
}

// 定义map任务信息传递接口
func MapTransmit(args *RPCArgs, reply *RPCReply) bool {
	//fmt.Printf("Map transmit start\n")
	ok := call(MapData, args, reply)
	if ok {

		//fmt.Printf("Map data transmitted\n")
		return true
	} else {
		//fmt.Printf("Map data transmit failed!\n")
		return false
	}
}

// 定义reduce任务信息传递接口
func ReduceTransmit(args *RPCArgs, reply *RPCReply) bool {
	//fmt.Printf("Reduce transmit start\n")
	ok := call(ReduceData, args, reply)
	if ok {
		//fmt.Printf("Reduce data transmitted\n")
		return true
	} else {
		//fmt.Printf("Reduce data transmit failed!\n")
		return false
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
		//fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		//fmt.Printf("call failed!\n")
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
