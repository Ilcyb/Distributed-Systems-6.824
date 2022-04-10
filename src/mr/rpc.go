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

type RequestArgs struct {
	WorkerId int
}

type TaskReply struct {
	HasTask        bool
	TaskType       int // 0:Map 1:Reduce
	TaskId         int
	FilesPath      []string
	ReduceNum      int
	FileNameFormat string
	Done           bool
	WorkerId       int
}

type SubmitArgs struct {
	WorkerId  int
	TaskId    int
	TaskType  int // 0:Map 1:Reduce
	FilesPath []string
}

type SubmitRelpy struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
