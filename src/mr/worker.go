package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	work(0, mapf, reducef)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func CallRequestTask(workerId int) (TaskReply, error) {
	args := RequestArgs{
		WorkerId: workerId,
	}
	reply := TaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, errors.New("call RequestTask failed")
	}
}

func CallSubmitTask(submitArgs SubmitArgs) error {
	// fmt.Printf("Worker:%d submit task:%+v\n", submitArgs.WorkerId, submitArgs)
	submitReply := SubmitRelpy{}
	ok := call("Coordinator.SubmitTask", &submitArgs, &submitReply)
	if ok {
		return nil
	} else {
		return errors.New("call SubmitTask failed")
	}
}

func work(workerId int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	currentDir, _ := os.Getwd()

	for {
		taskReply, err := CallRequestTask(workerId)
		if workerId == 0 {
			workerId = taskReply.WorkerId
		}
		if err != nil {
			log.Fatalf("Worker:%d\tError:%s\n", workerId, err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		if taskReply.Done {
			// fmt.Printf("Worker:%d Exited\n", workerId)
			return
		}
		if !taskReply.HasTask {
			time.Sleep(1 * time.Second)
			continue
		} else {
			// fmt.Printf("Worker:%d\tget task %+v\n", workerId, taskReply)
			if taskReply.TaskType == 0 {
				filename := taskReply.FilesPath[0]
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("Worker:%d\tcannot open %v\n", workerId, filename)
					time.Sleep(1 * time.Second)
					continue
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Worker:%d\tcannot read %v\n", workerId, filename)
					time.Sleep(1 * time.Second)
					continue
				}
				file.Close()
				kva := mapf(filename, string(content))
				kvReduceNum := make(map[int][]KeyValue, 0)
				for _, kv := range kva {
					hashReduceNo := ihash(kv.Key) % taskReply.ReduceNum
					if val, ok := kvReduceNum[hashReduceNo]; ok {
						kvReduceNum[hashReduceNo] = append(val, kv)
					} else {
						tmpKV := make([]KeyValue, 0, 1)
						tmpKV = append(tmpKV, kv)
						kvReduceNum[hashReduceNo] = tmpKV
					}
				}

				intermediateFiles := make([]string, 0, taskReply.ReduceNum)

				var saveErr error
				for reduceNo, kvList := range kvReduceNum {
					tempFile, err := ioutil.TempFile(currentDir, "mr-tmp-*")
					intermediateFileName := fmt.Sprintf(taskReply.FileNameFormat, taskReply.TaskId, reduceNo)

					if err != nil {
						log.Fatalf("Worker:%d\tcannot open temp file\n", workerId)
						saveErr = err
						break
					}

					enc := json.NewEncoder(tempFile)

					if encodeErr := enc.Encode(&kvList); encodeErr != nil {
						log.Fatalf("Worker:%d\tcannot encode kva\n", workerId)
						saveErr = encodeErr
						break
					}

					if err := os.Rename(tempFile.Name(), intermediateFileName); err != nil {
						log.Fatalf("Worker:%d\tcannot rename intermediate file %s\n", workerId, tempFile.Name())
						saveErr = err
						break
					}

					intermediateFiles = append(intermediateFiles, intermediateFileName)
				}
				if saveErr != nil {
					time.Sleep(1 * time.Second)
					continue
				}

				submitArgs := SubmitArgs{
					WorkerId:  workerId,
					TaskId:    taskReply.TaskId,
					TaskType:  taskReply.TaskType,
					FilesPath: intermediateFiles,
				}
				if err := CallSubmitTask(submitArgs); err != nil {
					log.Fatalf("Worker:%d\tcannot submit task:%d \n", workerId, taskReply.TaskId)
					time.Sleep(1 * time.Second)
					continue
				}

				// fmt.Printf("Worker:%d success submit task:%d\n", workerId, taskReply.TaskId)
			} else if taskReply.TaskType == 1 {
				kva := []KeyValue{}
				for _, fileName := range taskReply.FilesPath {
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatalf("Worker:%d\tcannot open %v\n", workerId, fileName)
						time.Sleep(1 * time.Second)
						continue
					}
					dec := json.NewDecoder(file)
					kv := []KeyValue{}
					if decErr := dec.Decode(&kv); decErr != nil {
						log.Fatalf("Worker:%d\tcannot decode %v\n", workerId, fileName)
						time.Sleep(1 * time.Second)
						continue
					}
					kva = append(kva, kv...)
				}

				sort.Sort(ByKey(kva))
				oname := fmt.Sprintf(taskReply.FileNameFormat, taskReply.TaskId)
				tempFile, err := ioutil.TempFile(currentDir, "mr-tmp-*")
				if err != nil {
					log.Fatalf("Worker:%d\tcannot create temp file\n", workerId)
					time.Sleep(1 * time.Second)
					continue
				}

				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

					i = j
				}
				if err := os.Rename(tempFile.Name(), oname); err != nil {
					log.Fatalf("Worker:%d\tcannot rename final file %s\n", workerId, tempFile.Name())
					time.Sleep(1 * time.Second)
					continue
				}

				submitArgs := SubmitArgs{
					WorkerId: workerId,
					TaskId:   taskReply.TaskId,
					TaskType: taskReply.TaskType,
				}

				if err := CallSubmitTask(submitArgs); err != nil {
					log.Fatalf("Worker:%d\tcannot submit task:%d \n", workerId, taskReply.TaskId)
					time.Sleep(1 * time.Second)
					continue
				}

				// fmt.Printf("Worker:%d success submit task:%d\n", workerId, taskReply.TaskId)

			} else {
				log.Fatalf("Worker:%d get unsupported task type:%d\n", workerId, taskReply.TaskType)
			}
		}
		time.Sleep(1 * time.Second)
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
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
