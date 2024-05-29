package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"syscall"
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerId := generateWorkerId()
	for {
		task := AskForTask(workerId)
		if task.TaskType == emptyTask {
			return
		}
		RunTask(task, mapf, reducef)
		DoneTask(task)
		time.Sleep(1 * time.Second)
	}
}

func RunTask(task *Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	//log.Println("enter RunTask()")
	switch task.TaskType {
	case mapTask:
		fd, err := os.Open(task.Filepath)
		if err != nil {
			log.Fatalf("cannot open %v", task.Filepath)
		}
		defer fd.Close()
		content, err := io.ReadAll(fd)
		if err != nil {
			log.Fatalf("read file:%v failed", task.Filepath)
		}
		intermediate := mapf(task.Filepath, string(content))
		// split intermediate KVs by nReduce
		parts := make([][]KeyValue, task.NReduce)
		for _, kv := range intermediate {
			idx := ihash(kv.Key) % task.NReduce
			parts[idx] = append(parts[idx], kv)
		}
		for i := 0; i < task.NReduce; i++ {
			kvs := parts[i]
			file := fmt.Sprintf("mr-%s-%d", task.WorkerId, i)
			tempFile := file + "-*"
			temp, err := os.CreateTemp("./", tempFile)
			if err != nil {
				log.Fatalf("cannot create temp file:%v", tempFile)
			}
			enc := json.NewEncoder(temp)
			for _, kv := range kvs {
				err := enc.Encode(kv)
				if err != nil {
					log.Fatalf("encode failed, err:%v", err)
				}
			}
			fd, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("cannot create or open file:%v", file)
			}
			temp.Seek(0, 0)
			err = syscall.Flock(int(fd.Fd()), syscall.LOCK_EX)
			if err != nil {
				log.Fatalf("get flock failed, %v", err)
			}
			_, err = io.Copy(fd, temp)
			if err != nil {
				log.Fatalf("copy %v to %v failed, err:%v", temp.Name(), file, err)
			}
			syscall.Flock(int(fd.Fd()), syscall.LOCK_UN)
			temp.Close()
			os.Remove(temp.Name())
			fd.Close()
		}
	case reduceTask:
		kvs := make([]KeyValue, 0, 50)
		reduceId := task.ReduceTaskId
		var partFiles []string
		for workerId := range task.DoneWorkers {
			partFiles = append(partFiles, fmt.Sprintf("mr-%s-%d", workerId, reduceId))
		}
		for i := range partFiles {
			fd, err := os.Open(partFiles[i])
			if err != nil {
				log.Fatalf("open part file:%v failed", partFiles[i])
			}
			defer func() {
				fd.Close()
				os.Remove(fd.Name())
			}()
			dec := json.NewDecoder(fd)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kvs = append(kvs, kv)
			}
		}
		sort.Sort(ByKey(kvs))
		out, _ := os.Create("mr-out-" + strconv.Itoa(reduceId))
		defer out.Close()
		i := 0
		for i < len(kvs) {
			j := i + 1
			for j < len(kvs) && kvs[j].Key == kvs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kvs[k].Value)
			}
			output := reducef(kvs[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(out, "%v %v\n", kvs[i].Key, output)

			i = j
		}

	default:
		return
	}
}

func AskForTask(workerId string) *Task {
	//log.Println("enter AskForTask()")
	args := GetTaskArgs{
		WorkerId: workerId,
	}
	reply := new(Task)
	ok := call("Coordinator.GetTask", &args, reply)
	if ok {
		//log.Printf("get task %+v\n", reply)
	} else {
		//log.Println("called failed")
	}
	return reply
}

func DoneTask(task *Task) {
	//log.Println("enter DoneTask()")
	args := DoneTaskArgs{
		TaskId:   task.TaskId,
		WorkerId: task.WorkerId,
	}
	ok := call("Coordinator.DoneTask", &args, nil)
	if ok {
		return
	} else {
		log.Println("called failed")
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
		os.Exit(1)
		//log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		fmt.Println(err)
	}
	return false
}

func generateWorkerId() string {
	return generateRandomString(6)
}
