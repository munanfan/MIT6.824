package mr

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
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

func sendHeartbeat(taskIndex int, taskType int, mutex *sync.Mutex, finish *bool, wg *sync.WaitGroup) {
	defer wg.Done()
	heartbeat := Heartbeat{
		TaskIndex: taskIndex,
		TaskType:  taskType,
	}
	mutex.Lock()
	state := *finish
	mutex.Unlock()
	for !(state) {
		reply := HearbeatReply{}
		err := callForHeartbeat(&heartbeat, &reply)
		if err != nil {
			//fmt.Println("coordinator can't solve heartbeat")
			break
		}
		time.Sleep(5 * time.Second)
		mutex.Lock()
		state = *finish
		mutex.Unlock()
	}
	if taskType == 0 {
		//fmt.Println("map task " + strconv.Itoa(taskIndex) + "finished!!!!")
	} else {
		//fmt.Println("reduce task " + strconv.Itoa(taskIndex) + "finished!!!!")
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// define the request and reply message
		requestInfo := RequestTask{}
		taskInfo := TaskInfo{}
		// invoke the rpc
		err := CallForTask(&requestInfo, &taskInfo)
		//fmt.Println("has task:", taskInfo.HasTask)
		//fmt.Println("task type:", taskInfo.MapTaskType)
		//fmt.Println("mapCount", taskInfo.MapCount)
		//fmt.Println("nReduce:", taskInfo.NReduce)
		wg := sync.WaitGroup{}
		if err == nil {
			if taskInfo.HasTask {
				mutex := sync.Mutex{}
				// start heartbeat routine
				var finish bool = false
				if taskInfo.MapTaskType {
					wg.Add(1)
					go sendHeartbeat(taskInfo.TaskIndex, 0, &mutex, &finish, &wg)
					//fmt.Println("receive a map task " + strconv.Itoa(taskInfo.TaskIndex))
					// do map task
					file, err := os.ReadFile(taskInfo.Filename)
					if err == nil {
						// read the file
						content := string(file)
						//fmt.Println("map task " + strconv.Itoa(taskInfo.TaskIndex) + " read data")
						// end the read, start to process map
						values := mapf(taskInfo.Filename, string(content))
						//fmt.Println("map task " + strconv.Itoa(taskInfo.TaskIndex) + " finish map process")
						reduceData := make([][]KeyValue, taskInfo.NReduce)
						for i := 0; i < taskInfo.NReduce; i++ {
							reduceData[i] = make([]KeyValue, 0)
						}
						// delivery different data to different slice
						for _, keyValue := range values {
							index := ihash(keyValue.Key) % taskInfo.NReduce
							reduceData[index] = append(reduceData[index], keyValue)
						}
						// save data to file
						//fmt.Println("map task " + strconv.Itoa(taskInfo.TaskIndex) + " save result to file")
						for i := 0; i < taskInfo.NReduce; i++ {
							// create file and save: mr-mapId-NReduce
							var name string = "mr-" + strconv.Itoa(taskInfo.TaskIndex) + "-" + strconv.Itoa(i)
							err := os.Remove("./" + name)
							if err != nil {
								//fmt.Println(err)
							}
							os.Create("./" + name)
							file, err := os.OpenFile("./"+name, os.O_RDWR, os.ModePerm)
							if err != nil {
								log.Fatal("open fail")
							} else {
								//fmt.Println("map task " + strconv.Itoa(taskInfo.TaskIndex) + " open file success")
								for j := 0; j < len(reduceData[i]); j++ {
									// save every KeyValue into file
									_, writeErr := file.WriteString(reduceData[i][j].Key + " " + reduceData[i][j].Value + "\n")
									if writeErr != nil {
										log.Fatal("write Error")
									}
								}
							}
							file.Close()
						}
					}
					//fmt.Println("map task " + strconv.Itoa(taskInfo.TaskIndex) + " send finish message start")
					workFinish := WorkFinish{
						TaskIndex: taskInfo.TaskIndex,
					}
					workFinishReply := WorkFinishReply{}
					err = callForFinish(&workFinish, &workFinishReply)
					if err != nil {
						//fmt.Println(err)
						//fmt.Println("call for Finish occur error")
					}
					//fmt.Println("map task " + strconv.Itoa(taskInfo.TaskIndex) + " send finish message end")
				} else {
					wg.Add(1)
					go sendHeartbeat(taskInfo.TaskIndex, 1, &mutex, &finish, &wg)
					//fmt.Println("receive a reduce task " + strconv.Itoa(taskInfo.TaskIndex))
					// do reduce task
					keyMap := make(map[string][]string)
					content := ""
					// read all file and convert into string
					for i := 0; i < taskInfo.MapCount; i++ {
						var name string = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskInfo.TaskIndex)
						//fmt.Println(name)
						file, err := os.ReadFile("./" + name)
						if err != nil {
							//fmt.Println("reduce task readFile occur error")
						}
						content += string(file)
					}
					lineList := strings.Split(content, "\n")
					for _, line := range lineList {
						if len(line) == 0 {
							continue
						}
						words := strings.Split(line, " ")
						_, ok := keyMap[words[0]]
						// KeyMap has key words[i]
						if ok {
							keyMap[words[0]] = append(keyMap[words[0]], words[1])
						} else {
							keyMap[words[0]] = []string{words[1]}
						}
					}
					// sort the key
					keySlice := make([]string, 0)
					for key, _ := range keyMap {
						keySlice = append(keySlice, key)
					}
					// solve all key and save to file
					//fmt.Println("reduce task " + strconv.Itoa(taskInfo.TaskIndex) + " save result to file")
					os.Remove("./mr-out-" + strconv.Itoa(taskInfo.TaskIndex))
					os.Create("./mr-out-" + strconv.Itoa(taskInfo.TaskIndex))
					file, err := os.OpenFile("./mr-out-"+strconv.Itoa(taskInfo.TaskIndex), os.O_RDWR, os.ModePerm)
					if err != nil {
						//fmt.Println("Sorry, reduce open file fail when save data")
					}
					for i := 0; i < len(keySlice); i++ {
						nums := reducef(keySlice[i], keyMap[keySlice[i]])
						//fmt.Println("key", keySlice[i], "reduce value", nums)
						_, err = file.WriteString(keySlice[i] + " " + nums + "\n")
						if err != nil {
							//fmt.Println(err)
							//fmt.Println("Sorry, reduce write file fail when save data")
						}
					}
					file.Close()
					workFinish := WorkFinish{
						TaskIndex: taskInfo.TaskIndex,
					}
					workFinishReply := WorkFinishReply{}
					err = callForFinish(&workFinish, &workFinishReply)
					if err != nil {
						//fmt.Println("call for Finish occur error")
					}
				}
				mutex.Lock()
				finish = true
				mutex.Unlock()
				wg.Wait()
			} else {
				time.Sleep(1 * time.Second)
			}
		} else {
			//fmt.Println("error occur, no reply from coordinator")
		}
	}

}

// send hearbeat to coordinator
func callForHeartbeat(heartbeat *Heartbeat, reply *HearbeatReply) error {
	ok := call("Coordinator.SolveHearBeat", &heartbeat, &reply)
	if !ok {
		return errors.New("no reply")
	}
	return nil
}

// send finish info to coordinator
func callForFinish(workFinish *WorkFinish, reply *WorkFinishReply) error {
	ok := call("Coordinator.ReceiveFinish", &workFinish, &reply)
	if !ok {
		return errors.New("no reply")
	}
	return nil
}

// CallForTask ask for a task
func CallForTask(requestInfo *RequestTask, taskInfo *TaskInfo) error {
	ok := call("Coordinator.AllocateTask", &requestInfo, &taskInfo)
	if !ok {
		return errors.New("no reply")
	}
	return nil
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
