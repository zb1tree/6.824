package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// 定义限时任务
type Timer map[string]time.Time

// 定义储存对象
type storage map[string]ByKey

type Coordinator struct {
	// Your definitions here.
	MapToDo       []string
	ReduceToDo    []string
	MapProcess    Timer
	ReduceProcess Timer
	MidResults    map[string][]string
	Result        storage
	NReduce       int
	Mutex         int
}

func init() { gob.Register(KeyValue{}); gob.Register(ByKey{}) }

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 为新加入的worker发送任务
func (c *Coordinator) JoinWorker(args *RPCArgs, reply *RPCReply) error {
	fmt.Printf("Worker join start\n")
	for c.Mutex == 1 {
		time.Sleep(time.Second)
	}
	c.Mutex = 1
	for {
		if len(c.MapToDo) != 0 { //仍有需要完成的任务
			reply.Task.Method = MAP
			reply.Task.Obj = c.MapToDo[0]
			c.MapProcess[c.MapToDo[0]] = time.Now()
			if len(c.MapToDo) > 0 {
				c.MapToDo = c.MapToDo[1:]
			}
			break
		} else if len(c.MapProcess) != 0 {
			for task := range c.MapProcess {
				if time.Since(c.MapProcess[task])*time.Second >= 10 {
					delete(c.MapProcess, task)
					c.MapToDo = append(c.MapToDo, task)
				}
			}
		} else if len(c.ReduceToDo) != 0 {
			reply.Task.Method = REDUCE
			reply.Task.Obj = RKeyValue{Key: c.ReduceToDo[0], Value: c.MidResults[c.ReduceToDo[0]][:]}
			c.ReduceProcess[c.ReduceToDo[0]] = time.Now()
			if len(c.ReduceToDo) > 0 {
				c.ReduceToDo = c.ReduceToDo[1:]
			}
			break
		} else if len(c.ReduceProcess) != 0 {
			for task := range c.ReduceProcess {
				if time.Since(c.ReduceProcess[task]).Seconds() >= 10 {
					delete(c.ReduceProcess, task)
					c.ReduceToDo = append(c.ReduceToDo, task)
				}
			}
		} else if c.Done() {
			reply.Task.Method = DONE
			break
		} else {
			time.Sleep(time.Second)
		}
	}
	c.Mutex = 0
	return nil
}

// 接收map任务结果
func (c *Coordinator) GetMapData(args *RPCArgs, reply *RPCReply) error {
	for c.Mutex == 1 {
		time.Sleep(time.Second)
	}
	c.Mutex = 1
	for _, kv := range args.Kvdata {
		_, ok := c.MidResults[kv.Key]
		if ok {
			c.MidResults[kv.Key] = append(c.MidResults[kv.Key], kv.Value)
		} else {
			c.MidResults[kv.Key] = []string{kv.Value}
			c.ReduceToDo = append(c.ReduceToDo, kv.Key)
		}
	}
	delete(c.MapProcess, args.Index)
	c.Mutex = 0
	return c.JoinWorker(args, reply)
}

// 接收reduce结果
func (c *Coordinator) GetReduceData(args *RPCArgs, reply *RPCReply) error {
	for c.Mutex == 1 {
		time.Sleep(time.Second)
	}
	c.Mutex = 1
	ind, _ := strconv.Atoi(args.Index)
	ind %= c.NReduce
	kv := args.Kvdata[0]
	ok := c.Result[strconv.Itoa(ind)]
	if ok != nil {
		c.Result[strconv.Itoa(ind)] = append(c.Result[strconv.Itoa(ind)], kv)
	} else {
		c.Result[strconv.Itoa(ind)] = ByKey{kv}
	}
	delete(c.ReduceProcess, kv.Key)
	fmt.Printf("Reduce to do:%d reduce doing:%d", len(c.ReduceToDo), len(c.MapProcess))
	c.Mutex = 0
	return c.JoinWorker(args, reply)
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
// 任务结束输出结果
func (c *Coordinator) Done() bool {
	ret := false
	obase := "mr-out-"
	var ofile *os.File
	if len(c.MapToDo) == 0 && len(c.ReduceToDo) == 0 && len(c.MapProcess) == 0 && len(c.MapProcess) == 0 {
		ret = true
		for i := 0; i < c.NReduce; i++ {
			sort.Sort(c.Result[strconv.Itoa(i)])
			ofile, _ = os.Create(obase + strconv.Itoa(i))
			for _, kv := range c.Result[strconv.Itoa(i)] {
				fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			}
		}
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapToDo = append(c.MapToDo, files...)
	c.MapProcess = Timer{}
	c.ReduceToDo = []string{}
	c.ReduceProcess = Timer{}
	c.MidResults = make(map[string][]string)
	c.NReduce = nReduce
	c.Result = storage{}
	c.Mutex = 0
	// Your code here.
	c.server()
	return &c
}
