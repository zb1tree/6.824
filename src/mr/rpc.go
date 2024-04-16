package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RPCArgs struct {
	event  int
	kvdata ByKey
}

//定义任务，method表示采取的方法，0:map 1:redduce
type task struct {
	method int
	obj    string
}
type RPCReply struct {
	task   task
	kvdata ByKey
}

//定义map任务信息传递接口
func MapTransmit(ByKey, *RPCArgs, *RPCReply) bool {
	return true
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
