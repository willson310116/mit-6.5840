package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskType int

const (
	ALL_TASK_DONE TaskType = iota
	MAP
	REDUCE
)

const (
	MAP_TEMP_DIR = "./map-tmp-output/"
)

type Task struct {
	Type        TaskType
	MapFilePath string
	Id          int
}

type TaskRequest struct {
	WorkerPid int
}

type TaskReply struct {
	Task      Task
	NumReduce int
	NumMap    int
}

type TaskDoneRequest struct {
	WorkerPid int
}

type TaskDoneReply struct {
	// nothing to reply
}

func GetReduceFilePath(map_id int, reduce_id int) string {
	return fmt.Sprintf("%smr-%d-%d", MAP_TEMP_DIR, map_id, reduce_id)
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
