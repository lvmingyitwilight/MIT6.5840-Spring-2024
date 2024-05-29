package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int

const (
	emptyTask = iota
	mapTask
	reduceTask
)

const (
	mapPhase = iota
	reducePhase
	over
)

func (t TaskType) String() string {
	switch t {
	case mapTask:
		return "Map"
	case reduceTask:
		return "Reduce"
	default:
		return "Unknown"
	}
}

type Task struct {
	TaskId       string
	TaskType     TaskType
	NReduce      int
	WorkerId     string              // only mapTask used
	Filepath     string              // only mapTask used
	ReduceTaskId int                 // only reduceTask used
	DoneWorkers  map[string]struct{} // only reduceTask used
}

type RunningTask struct {
	*Task
	CreateTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu sync.RWMutex

	counter uint32
	target  uint32
	nReduce int
	state   uint32 // state: 0-mapPhase. 1-reducePhase. 2-over

	files       chan string
	reduceTasks chan int

	runningTasks map[string]*RunningTask
	doneWorkers  map[string]struct{}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *Task) error {
	task := c.getIdleTask(args.WorkerId)
	reply.TaskId = task.TaskId
	reply.TaskType = task.TaskType
	reply.Filepath = task.Filepath
	reply.WorkerId = task.WorkerId
	reply.ReduceTaskId = task.ReduceTaskId
	reply.NReduce = c.nReduce
	reply.DoneWorkers = c.doneWorkers
	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *struct{}) error {
	key := args.TaskId
	if key == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.runningTasks[key]
	if !ok {
		return nil
	}
	if task.TaskType == mapTask {
		c.doneWorkers[task.WorkerId] = struct{}{}
	}
	delete(c.runningTasks, key)

	if c.state == over {
		return nil
	}

	c.counter++
	if c.target == c.counter {
		if c.state == mapPhase {
			c.state = reducePhase
			c.counter = 0
			c.target = uint32(c.nReduce)
		} else if c.state == reducePhase {
			c.state = over
		}
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
	//log.Println("Coordinator start listening...")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	//log.Printf("%v %v/%v\n", c.state, c.counter, c.target)
	// Your code here.
	if atomic.LoadUint32(&c.state) == over {
		ret = true
	}
	return ret
}

func (c *Coordinator) getIdleTask(workerId string) *Task {
	for {
		switch atomic.LoadUint32(&c.state) {
		case mapPhase:
			select {
			case file := <-c.files:
				task := &Task{
					TaskType: mapTask,
					TaskId:   generateTaskId(),
					WorkerId: workerId,
					Filepath: file,
				}
				key := task.TaskId
				c.mu.Lock()
				c.runningTasks[key] = &RunningTask{
					task,
					time.Now(),
				}
				c.mu.Unlock()
				return task
			}
		case reducePhase:
			select {
			case id := <-c.reduceTasks:
				task := &Task{
					TaskType:     reduceTask,
					TaskId:       generateTaskId(),
					ReduceTaskId: id,
					DoneWorkers:  c.doneWorkers,
				}
				key := task.TaskId
				c.mu.Lock()
				c.runningTasks[key] = &RunningTask{
					task,
					time.Now(),
				}
				c.mu.Unlock()
				return task
			}
		case over:
			return &Task{
				TaskType: emptyTask,
			}
		}
	}
}

func (c *Coordinator) watchRunningTasks() {
	ticker := time.NewTicker(5 * time.Second)
	for {
		if atomic.LoadUint32(&c.state) == over {
			ticker.Stop()
			return
		}
		select {
		case <-ticker.C:
			c.mu.Lock()
			for k, v := range c.runningTasks {
				if v.CreateTime.Add(10 * time.Second).Before(time.Now()) {
					switch v.TaskType {
					case mapTask:
						c.files <- v.Filepath
					case reduceTask:
						c.reduceTasks <- v.ReduceTaskId
					}
					delete(c.runningTasks, k)
				}
			}
			c.mu.Unlock()
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		nReduce:      nReduce,
		target:       uint32(len(files)),
		files:        make(chan string, len(files)),
		reduceTasks:  make(chan int, nReduce),
		runningTasks: make(map[string]*RunningTask),
		doneWorkers:  make(map[string]struct{}),
	}
	for i := range files {
		c.files <- files[i]
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- i
	}
	go c.watchRunningTasks()
	c.server()
	return &c
}

func generateTaskId() string {
	return "task-" + generateRandomString(6)
}
