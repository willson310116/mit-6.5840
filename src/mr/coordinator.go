package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Mode int

const (
	InMAP Mode = iota
	InREDUCE
	DONE
)

type Coordinator struct {
	nMap        int
	nReduce     int
	Mode        Mode
	TaskChannel chan Task
	TaskAssined map[int]Task
	doneMap     int
	doneReduce  int
	mu          sync.Mutex
}

func (c *Coordinator) SwtichMode() {
	// fmt.Println("Switch mode")
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Mode == InMAP {
		c.Mode = InREDUCE
		// create Reduce tasks
		// fmt.Println("Create Reduce tasks ...")
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type: REDUCE,
				Id:   i,
			}
			c.TaskChannel <- task
		}
	} else if c.Mode == InREDUCE {
		c.Mode = DONE
		fmt.Println("All tasks are done")
	}
	return
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	if (c.Mode == InMAP && c.doneMap == c.nMap) ||
		(c.Mode == InREDUCE && c.doneReduce == c.nReduce) {
		c.SwtichMode()
	}
	if c.Mode == DONE {
		return nil
	}

	assignedTask := <-c.TaskChannel
	// fmt.Printf("Server assigning %v to pid %v\n", assignedTask.MapFilePath, args.WorkerPid)
	reply.Task = assignedTask
	reply.NumMap = c.nMap
	reply.NumReduce = c.nReduce
	reply.NumReduce = c.nReduce
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TaskAssined[args.WorkerPid] = assignedTask
	return nil
}

func (c *Coordinator) NotifyTaskDone(args *TaskDoneRequest, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if task, ok := c.TaskAssined[args.WorkerPid]; ok {
		// fmt.Printf("Notified %v is done from pid: %v\n", task.Type, args.WorkerPid)
		switch task.Type {
		case MAP:
			c.doneMap += 1
		case REDUCE:
			c.doneReduce += 1
		default:
			fmt.Println(reply)
			fmt.Printf("Received invalid task\n")
		}
	} else {
		fmt.Printf("No task assgined to pid: %v\n", args.WorkerPid)
	}
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
	if c.Mode == DONE {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		Mode:        InMAP,
		doneMap:     0,
		doneReduce:  0,
		TaskChannel: make(chan Task, nReduce),
		TaskAssined: make(map[int]Task)}

	// create Map tasks and send into task channel
	for i, file := range files {
		task := Task{
			Type:        MAP,
			MapFilePath: file,
			Id:          i}
		c.TaskChannel <- task
	}

	// close(c.TaskChannel)

	// TODO: create another thread to check execution time on each task
	// assign the task to other worker if it exceeds time limit

	c.server()
	return &c
}
