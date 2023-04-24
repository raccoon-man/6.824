package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "os"
import "io/ioutil"
import "strings"
import "sort"
//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func tmpMapOutFile(wokerid string, TaskIndex int, ReduceIndex int) string {
	return fmt.Sprintf("Maptmp%s-%d-%d", wokerid, TaskIndex, ReduceIndex)
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	id := strconv.Itoa(os.Getpid())
	log.Printf("woker %s started\n", id)

	var Previous_Task_Tpye string
	var Previous_Task_Index int
	Previous_Task_Index = -1
	Previous_Task_Tpye = ""
	for{
		reply := AskForTask(id, Previous_Task_Tpye, Previous_Task_Index) // 发送rpc到coordinator
		log.Printf("the Task%s-%d arrive the worker%s", reply.TaskType, reply.TaskIndex, id)
		if reply.TaskType == "" {
			log.Printf("coordinator send a finish signal")
			break;
		}
		if reply.TaskType == "Map" {

			file, err := os.Open(reply.MapInputFileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapInputFileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapInputFileName)
			}
			file.Close()

			kva := mapf(reply.MapInputFileName, string(content))
			hashkva := make(map[int][]KeyValue)
			nReduce := reply.ReduceTaskNumber
			for _, kv := range kva{
				hashed := ihash(kv.Key) % nReduce
				hashkva[hashed] = append(hashkva[hashed], kv)
			}
			for i := 0; i < nReduce; i ++ {
				mpofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i)) 
				for _, kv := range hashkva[i] {
					fmt.Fprintf(mpofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				mpofile.Close()
			}

			Previous_Task_Tpye = reply.TaskType
			Previous_Task_Index = reply.TaskIndex

		} else if reply.TaskType == "Reduce" {
			var lines[] string
			for mi := 0; mi < reply.MapTaskNumber; mi ++ {
				inputfile := finalMapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputfile)
				if err != nil {
					log.Fatalf("Failed to open map output file %s: %e", inputfile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e", inputfile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)

			}
			interdata := []KeyValue {}
			for _, line := range lines {
				if strings.TrimSpace(line) == ""{ 
					continue;
				}
				parts := strings.Split(line, "\t")
				interdata = append(interdata, KeyValue{
					Key : parts[0],
					Value : parts[1],
				})
			}
			sort.Sort(ByKey(interdata))
			ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskIndex))

			i := 0
			for i < len(interdata) {
				j := i + 1
				for j < len(interdata) && interdata[j].Key == interdata[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, interdata[k].Value)
				}
				output := reducef(interdata[i].Key, values)
		
				fmt.Fprintf(ofile, "%v %v\n", interdata[i].Key, output)
		
				i = j
			}
			ofile.Close()
			Previous_Task_Tpye = reply.TaskType
			Previous_Task_Index = reply.TaskIndex
		}
		
	}
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
	ok := call("Coordinator.Example", &args, &reply)  // 建立连接并调用Coordinator的Example函数
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func AskForTask(workerid string, pre_task_type string, pre_task_index int) AskForTaskReply {
	args := AskForTaskArgs{
		WorkerId : workerid,
		PreviousTaskTpye : pre_task_type,
		PreviousTaskIndex : pre_task_index,
	}
	reply := AskForTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)  
	if ok {
		// reply.Y should be 100.
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return AskForTaskReply{}
	}

}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
