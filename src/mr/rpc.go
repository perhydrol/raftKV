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

type taskType int

const (
	taskMap = iota
	taskReduce
	taskWait
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	taskInfo
}

type taskInfo struct {
	Task        taskType
	Input       string
	ReduceCount int
	workerId    int
}

type ReturnTaskArgs struct {
	Task     taskType
	Input    string
	Output   []string
	workerId int
}

type ReturnTaskReply struct {
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
