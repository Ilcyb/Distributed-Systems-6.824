package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	pieces                int
	nReduce               int
	mapTasks              map[int]task
	requestedMapTasks     map[int]task
	mapFinished           bool
	intermediateFilesPath []string
	reduceTasks           map[int]task
	requestedReduceTasks  map[int]task
	reduceFinished        bool
	workerCount           int
	mutex                 sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

type task struct {
	id        int
	filesPath []string
	beginTime time.Time
	workerId  int
	status    int // 0:任务未被领取 1:任务已被领取
}

func (c *Coordinator) selfCheck() {

	for {
		// fmt.Printf("Coordinator Self-Check begin\n")
		c.mutex.Lock()
		currentTime := time.Now()
		if !c.mapFinished {
			for k, v := range c.requestedMapTasks {
				if currentTime.Sub(v.beginTime) >= 10*time.Second {
					delete(c.requestedMapTasks, k)
					c.mapTasks[k] = v
					// fmt.Printf("Coordinator reset timeout task:%+v\n", v)
				}
			}
			if len(c.mapTasks) == 0 && len(c.requestedMapTasks) == 0 {
				c.mapFinished = true
				intermediateReg := regexp.MustCompile("mr-[0-9]+-([0-9]+)")
				reduceFilesPath := make([][]string, c.nReduce)
				for i := 0; i < c.nReduce; i++ {
					reduceFilesPath[i] = make([]string, 0)
				}
				for _, filePath := range c.intermediateFilesPath {
					regResult := intermediateReg.FindStringSubmatch(filePath)
					reduceId, _ := strconv.Atoi(regResult[1])
					reduceFilesPath[reduceId] = append(reduceFilesPath[reduceId], filePath)
				}
				for i := 0; i < c.nReduce; i++ {
					newReduceTask := task{
						id:        i,
						filesPath: reduceFilesPath[i],
					}
					c.reduceTasks[i] = newReduceTask
				}
			}
		} else if !c.reduceFinished {
			for k, v := range c.requestedReduceTasks {
				if currentTime.Sub(v.beginTime) >= 10*time.Second {
					delete(c.requestedReduceTasks, k)
					c.reduceTasks[k] = v
				}
			}
			if len(c.reduceTasks) == 0 && len(c.requestedReduceTasks) == 0 {
				c.reduceFinished = true
			}
		}

		c.mutex.Unlock()
		// fmt.Printf("Coordinator Self-Check end\n")
		time.Sleep(time.Second * 1)
	}
}

func getTask(tasks *map[int]task) task {
	for k, v := range *tasks {
		delete(*tasks, k)
		return v
	}
	return task{}
}

func (c *Coordinator) RequestTask(args *RequestArgs, reply *TaskReply) error {
	c.mutex.Lock()

	if args.WorkerId == 0 {
		c.workerCount++
		reply.WorkerId = c.workerCount
		args.WorkerId = c.workerCount
	}

	if !c.mapFinished {
		if len(c.mapTasks) != 0 {
			mapTask := getTask(&c.mapTasks)
			mapTask.beginTime = time.Now()
			mapTask.workerId = args.WorkerId
			reply.FilesPath = mapTask.filesPath
			reply.HasTask = true
			reply.TaskType = 0
			reply.TaskId = mapTask.id
			reply.ReduceNum = c.nReduce
			reply.FileNameFormat = "mr-%d-%d"
			c.requestedMapTasks[mapTask.id] = mapTask
			c.mutex.Unlock()
			return nil
		}
	} else if !c.reduceFinished {
		if len(c.reduceTasks) != 0 {
			reduceTask := getTask(&c.reduceTasks)
			// fmt.Printf("Coordinator get reduce task:%+v\n", reduceTask)
			reduceTask.beginTime = time.Now()
			reduceTask.workerId = args.WorkerId
			reply.FilesPath = reduceTask.filesPath
			reply.HasTask = true
			reply.TaskType = 1
			reply.TaskId = reduceTask.id
			reply.FileNameFormat = "mr-out-%d"
			c.requestedReduceTasks[reduceTask.id] = reduceTask
			c.mutex.Unlock()
			return nil
		}

	}
	reply.HasTask = false
	c.mutex.Unlock()
	reply.Done = c.Done()
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitArgs, reply *SubmitRelpy) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// fmt.Printf("receive submit:%+v\n", args)

	if args.TaskType == 0 {
		if submitTask, ok := c.requestedMapTasks[args.TaskId]; ok {
			if submitTask.workerId == args.WorkerId {
				c.intermediateFilesPath = append(c.intermediateFilesPath, args.FilesPath...)
				delete(c.requestedMapTasks, submitTask.id)
			}
		}
	} else if args.TaskType == 1 {
		if submitTask, ok := c.requestedReduceTasks[args.TaskId]; ok {
			if submitTask.workerId == args.WorkerId {
				delete(c.requestedReduceTasks, submitTask.id)
			}
		}
	} else {
		return errors.New("unsupported task")
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	ret = c.mapFinished && c.reduceFinished
	c.mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.pieces = len(files)
	c.nReduce = nReduce
	mapTasks := make(map[int]task, len(files))
	for i, file := range files {
		filesPath := make([]string, 1)
		filesPath[0] = file
		newTask := task{
			id:        i,
			filesPath: filesPath,
			status:    0,
		}
		mapTasks[i] = newTask
	}
	c.mapTasks = mapTasks
	c.requestedMapTasks = make(map[int]task, 0)
	c.mapFinished = false
	c.intermediateFilesPath = make([]string, 0, c.pieces*c.nReduce)
	c.reduceTasks = make(map[int]task, 0)
	c.requestedReduceTasks = make(map[int]task, 0)
	c.reduceFinished = false
	c.mutex = sync.Mutex{}
	c.workerCount = 0

	go c.selfCheck()

	c.server()
	return &c
}
