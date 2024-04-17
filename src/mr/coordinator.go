package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)
type Timer map[string]time.Time
type Coordinator struct {
	// Your definitions here.
	MapToDo []string
	ReduceToDo []string
	MapProcess Timer
	ReduceProcess Timer
	MidResults map[string][]string
	result ByKey
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
//为新加入的worker发送任务
func (c *Coordinator)JoinWorker(args *RPCArgs,reply *RPCReply) error{
	if len(c.MapToDo)!=0{//仍有需要完成的任务
		reply.task.method=MAP
		reply.task.obj=c.MapToDo[0]
		c.MapProcess[c.MapToDo[0]]=time.now()
		c.MapToDo=c.MapToDo[1:]
	}elif len(c.ReduceToDo)!=0{
		reply.task.method=REDUCE
		reply.task.obj=RKeyValue{Key:c.ReduceToDo[0],Value:c.MidResults[c.ReduceToDo][:]}
		c.ReduceProcess[c.ReduceToDo[0]]=time.now()
		c.ReduceToDo=c.ReduceToDo[1:]
	}elif c.Done(){
		reply.task.event=over
	}else{//任务均被占用的情况下检查超时任务
		if len(c.MapProcess)!=0{
			for task:= range c.MapProcess{
				if time.since(c.MapProcess[task]).Second()>=10{
					delete(c.MapProcess,task)
					c.MapToDo=append(c.MapToDo,task)
				}
			}
		}
		if len(c.ReduceProcess)!=0{
			for task:=range c.ReduceProcess{
				if time.since(c.ReduceProcess[task]).Second>=10{
					delete(c.ReduceProcess[task])
					c.ReduceToDo=append(c.ReduceToDo,task)
				}
			}
		}
		return c.JoinWorker(args,reply)
	}
	return nil
}
//接收map任务结果
func (c *Coordinator)GetMapData(args *RPCArgs,reply *RPCReply) error{
	for kv=:args.data{
		_,ok:=c.MidResults[kv.Key]
		if ok{
			c.MidResults[kv.Key]=append(c.MidResults[kv.Key],kv.Value)
		}else{
			c.MidResults[kv.Key]=[]string{kv.Value}
		}
	}
}
//接收reduce结果
func (c *Coordinator)GetReduceData(args *RPCArgs,reply *RPCReply) error{
	kv=args.data.(KeyValue)
	c.result=append(c.result,kv)
}
//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//任务结束输出结果
func (c *Coordinator) Done() bool {
	ret := false
	if len(c.MapToDo)==0 && len(c.ReduceToDo)==0 && len(c.MapProcess)==0 &&len(c.MapProcess)==0{
		ret=true
		sort.Sort(c.result)
		oname := "mr-out-0"
		ofile, _ := os.Create(oname)
		for kv:=range c.result{
			fmt.Fprintf(ofile,"%v %v",kv.Key,kv.Value)
		}
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapToDo=append(MapToDo,files...)
	c.MapIndex=0
	c.ReduceIndex=0
	// Your code here.
	c.server()
	return &c
}
