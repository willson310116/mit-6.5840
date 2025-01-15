package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

func PartitionKva(kva []KeyValue, nReduce int) map[int][]KeyValue {
	hashKvaMap := make(map[int][]KeyValue)
	for _, kv := range kva {
		// hash(key) mod R
		hashIdx := ihash(kv.Key) % nReduce
		hashKvaMap[hashIdx] = append(hashKvaMap[hashIdx], kv)
	}
	return hashKvaMap
}

func ReadFile(filepath string) []byte {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	file.Close()
	return content
}

func WriteFilesForReduce(hashKvaMap map[int][]KeyValue, nReduce int, taskId int) {
	for i := 0; i < nReduce; i++ {
		outputName := fmt.Sprintf("%smr-%d-%d", MAP_TEMP_DIR, taskId, i)
		outputFile, err := os.Create(outputName)
		if err != nil {
			log.Fatalf("cannot create %v", outputFile)
		}
		for _, kv := range hashKvaMap[i] {
			fmt.Fprintf(outputFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outputFile.Close()
	}
}

func ProcessMapTask(mapf func(string, string) []KeyValue, reply TaskReply, pid int) {
	defer func() {
		args := TaskDoneRequest{pid}
		reply := TaskDoneReply{}
		ok := call("Coordinator.NotifyTaskDone", &args, &reply)
		if !ok {
			fmt.Println("Coordinator.NotifyTaskDone fails")
		}
	}()
	content := ReadFile(reply.Task.MapFilePath)
	kva := mapf(reply.Task.MapFilePath, string(content))
	hashKvaMap := PartitionKva(kva, reply.NumReduce)
	WriteFilesForReduce(hashKvaMap, reply.NumReduce, reply.Task.Id)
}

func ProcessReduceTask(reducef func(string, []string) string, reply TaskReply, pid int) {
	defer func() {
		args := TaskDoneRequest{pid}
		reply := TaskDoneReply{}
		ok := call("Coordinator.NotifyTaskDone", &args, &reply)
		if !ok {
			fmt.Println("Coordinator.NotifyTaskDone fails")
		}
	}()
	// read kv pairs from all map tmp files
	var all_strings []string
	for i := 0; i < reply.NumMap; i++ {
		map_tmp_file_path := GetReduceFilePath(i, reply.Task.Id)
		content := ReadFile(map_tmp_file_path)
		all_strings = append(all_strings, strings.Split(string(content), "\n")...)
	}
	var kvas []KeyValue
	for _, str := range all_strings {
		// get rid of empty lines
		if strings.TrimSpace(str) == "" {
			continue
		}
		kva := strings.Split(str, "\t")
		kvas = append(kvas, KeyValue{
			Key:   kva[0],
			Value: kva[1]})
	}
	sort.Sort(ByKey(kvas))
	// apply reducef to aggregated values
	i := 0
	outputName := fmt.Sprintf("mr-out-%d", reply.Task.Id)
	outputFile, err := os.Create(outputName)
	if err != nil {
		log.Fatalf("cannot create %v", outputFile)
	}

	for i < len(kvas) {
		j := i + 1
		for j < len(kvas) && kvas[j].Key == kvas[i].Key {
			j += 1
		}
		var same_values []string
		for k := i; k < j; k++ {
			same_values = append(same_values, kvas[k].Value)
		}
		reduce_output := reducef(kvas[i].Key, same_values)
		fmt.Fprintf(outputFile, "%v %v\n", kvas[i].Key, reduce_output)
		i = j
	}
	outputFile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	err := os.MkdirAll(MAP_TEMP_DIR, 0755)
	if err != nil {
		log.Fatalf("Failed to create directories: %v", err)
	}

	// Your worker implementation here.
	pid := os.Getpid()
	for {
		args := TaskRequest{WorkerPid: pid}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			fmt.Println("Request task failed, re-try")
			time.Sleep(time.Second)
			continue
		} else {
			switch reply.Task.Type {
			case MAP:
				// fmt.Printf("Received MAP task\n")
				fmt.Printf("Processing %s\n", reply.Task.MapFilePath)
				// fmt.Println(reply.Task)
				ProcessMapTask(mapf, reply, pid)
				// return
			case REDUCE:
				fmt.Printf("Received REDUCE task\n")
				ProcessReduceTask(reducef, reply, pid)
				// return
			default:
				fmt.Println(reply)
				fmt.Printf("Received invalid task\n")
				return
			}
		}
		time.Sleep(time.Second)
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
