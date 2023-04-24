package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"
import "math"
type Task struct {
	Type string
	Index int
	MapInputFile string
	WorkerId string
	DeadLine time.Time
}

type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex // 互斥锁
	stage string
	nMap int
	nReduce int
	tasks map[string]Task
	availableTasks chan Task 
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

func GetTaskId(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
func finalMapOutFile(TaskIndex int, ReduceIndex int) string{
	return fmt.Sprintf("Mapfinal%d-%d", TaskIndex, ReduceIndex)
}
func tmpReduceOutFile(workerid string, TaskIndex int) string{
	return fmt.Sprintf("mr-out%s-%d", workerid, TaskIndex)
}
func finalReduceOutFile(TaskIndex int) string{
	return fmt.Sprintf("mr-out%d", TaskIndex)
}

func (c *Coordinator) transit(){
	if c.stage == "Map" {
		log.Printf("start Reduce stage")
		c.stage = "Reduce"
		for i := 0; i < c.nReduce; i ++ {
			task := Task{
				Type : "Reduce",
				Index : i,
			}
			c.tasks[GetTaskId(task.Type, task.Index)] = task;
			c.availableTasks <- task
		}
		
	} else if c.stage == "Reduce"{
		log.Printf("all Reduce Task completed")
		close(c.availableTasks)
		c.stage = ""
	}
}

func (c *Coordinator) AssignTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	if args.PreviousTaskTpye != "" {
		c.lock.Lock()
		PreviousTaskId := GetTaskId(args.PreviousTaskTpye, args.PreviousTaskIndex)
		if task, exists := c.tasks[PreviousTaskId]; exists && task.WorkerId == args.WorkerId {
			log.Printf("task %s is finished on worker %s", PreviousTaskId, task.WorkerId)
			if args.PreviousTaskTpye == "Map" {
				for ri := 0; ri < c.nReduce; ri ++ {
					err := os.Rename(tmpMapOutFile(args.WorkerId, args.PreviousTaskIndex, ri), finalMapOutFile(args.PreviousTaskIndex, ri))
					if err != nil {
						log.Fatalf(
                            "Failed to mark map output file `%s` as final: %e",
                            tmpMapOutFile(args.WorkerId, args.PreviousTaskIndex, ri), err)
                    }	
				}
			}else if args.PreviousTaskTpye == "Reduce" {
				err := os.Rename(
                    tmpReduceOutFile(args.WorkerId, args.PreviousTaskIndex),
                    finalReduceOutFile(args.PreviousTaskIndex))
                if err != nil {
                    log.Fatalf(
                        "Failed to mark reduce output file `%s` as final: %e",
                        tmpReduceOutFile(args.WorkerId, args.PreviousTaskIndex), err)
                }
			}
			
		}
		delete(c.tasks, PreviousTaskId)
		if len(c.tasks) == 0 {
			c.transit()
		}
		c.lock.Unlock()
	}


	task, ok := <- c.availableTasks
	if !ok {
		return nil
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	task.WorkerId = args.WorkerId
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.tasks[GetTaskId(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFileName = task.MapInputFile
	reply.MapTaskNumber = c.nMap
	reply.ReduceTaskNumber = c.nReduce

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
	if c.stage == "" {
		ret = true
	}


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage : "Map",
		nMap  : len(files),
		nReduce : nReduce,
		tasks : make(map[string]Task),
		availableTasks : make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	
	// Your code here.
	for i, file := range files{
		task := Task{
			Type : "Map",
			Index : i,
			MapInputFile : file,
		}
		c.tasks[GetTaskId(task.Type, task.Index)] = task
		c.availableTasks <- task
 	}
	log.Printf("coordinator started\n")
	c.server() // 启动服务器

	go func() {
		for{
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks{
				if task.WorkerId != "" && time.Now().After(task.DeadLine) {
					log.Printf("Worker %d is time out, task%s-%s assigned again", task.WorkerId, task.Type, task.Index)
					task.WorkerId = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()	
		}
	}()
	return &c
}
