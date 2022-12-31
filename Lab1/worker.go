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

// KeyValue 记录单词以及对应的数量
type KeyValue struct {
	Key   string
	Value string
}

// 使用 ihash(key) % NReduce 来确定该key属于哪一个reduce块
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// sendHeartbeat
/*
	用于向Coordinate发送RPC请求
*/
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
		reply := HeartbeatReply{}
		err := callForHeartbeat(&heartbeat, &reply)
		if err != nil {
			break
		}
		// 请求失败等待5秒再次请求
		time.Sleep(5 * time.Second)
		mutex.Lock()
		state = *finish
		mutex.Unlock()
	}
}

// Worker
/*
	类似worker线程
	不停地请求任务并且处理任务
*/
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// define the request and reply message
		requestInfo := RequestTask{}
		taskInfo := TaskInfo{}
		// 请求一个任务
		err := CallForTask(&requestInfo, &taskInfo)
		wg := sync.WaitGroup{}
		if err == nil {
			if taskInfo.HasTask {
				mutex := sync.Mutex{}
				// 设置flag记录任务执行情况
				var finish bool = false
				if taskInfo.MapTaskType {
					wg.Add(1)
					go sendHeartbeat(taskInfo.TaskIndex, 0, &mutex, &finish, &wg)
					// 读取文件
					file, err := os.ReadFile(taskInfo.Filename)
					if err == nil {
						content := string(file)
						// 执行map任务，返回KeyValue列表
						values := mapf(taskInfo.Filename, string(content))
						// 按照key分配到不同的reduce中
						reduceData := make([][]KeyValue, taskInfo.NReduce)
						for i := 0; i < taskInfo.NReduce; i++ {
							reduceData[i] = make([]KeyValue, 0)
						}
						for _, keyValue := range values {
							index := ihash(keyValue.Key) % taskInfo.NReduce
							reduceData[index] = append(reduceData[index], keyValue)
						}
						// 保存map结果到不同的分片中
						for i := 0; i < taskInfo.NReduce; i++ {
							var name string = "mr-" + strconv.Itoa(taskInfo.TaskIndex) + "-" + strconv.Itoa(i)
							err := os.Remove("./" + name)
							os.Create("./" + name)
							file, err := os.OpenFile("./"+name, os.O_RDWR, os.ModePerm)
							if err != nil {
								log.Fatal("open fail")
							} else {
								for j := 0; j < len(reduceData[i]); j++ {
									_, writeErr := file.WriteString(reduceData[i][j].Key + " " + reduceData[i][j].Value + "\n")
									if writeErr != nil {
										log.Fatal("write Error")
									}
								}
							}
							file.Close()
						}
					}
					// 发送任务完成请求
					workFinish := WorkFinish{
						TaskIndex: taskInfo.TaskIndex,
					}
					workFinishReply := WorkFinishReply{}
					err = callForFinish(&workFinish, &workFinishReply)
				} else {
					wg.Add(1)
					go sendHeartbeat(taskInfo.TaskIndex, 1, &mutex, &finish, &wg)
					keyMap := make(map[string][]string)
					content := ""
					// 读取文件并且转换为string
					for i := 0; i < taskInfo.MapCount; i++ {
						var name string = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskInfo.TaskIndex)
						file, _ := os.ReadFile("./" + name)
						content += string(file)
					}
					lineList := strings.Split(content, "\n")
					for _, line := range lineList {
						if len(line) == 0 {
							continue
						}
						words := strings.Split(line, " ")
						_, ok := keyMap[words[0]]
						if ok {
							keyMap[words[0]] = append(keyMap[words[0]], words[1])
						} else {
							keyMap[words[0]] = []string{words[1]}
						}
					}
					// 处理Key
					keySlice := make([]string, 0)
					for key, _ := range keyMap {
						keySlice = append(keySlice, key)
					}
					// 保存到文件
					os.Remove("./mr-out-" + strconv.Itoa(taskInfo.TaskIndex))
					os.Create("./mr-out-" + strconv.Itoa(taskInfo.TaskIndex))
					file, _ := os.OpenFile("./mr-out-"+strconv.Itoa(taskInfo.TaskIndex), os.O_RDWR, os.ModePerm)
					for i := 0; i < len(keySlice); i++ {
						nums := reducef(keySlice[i], keyMap[keySlice[i]])
						_, err = file.WriteString(keySlice[i] + " " + nums + "\n")
					}
					file.Close()
					// 发送任务完成请求
					workFinish := WorkFinish{
						TaskIndex: taskInfo.TaskIndex,
					}
					workFinishReply := WorkFinishReply{}
					err = callForFinish(&workFinish, &workFinishReply)
				}
				mutex.Lock()
				finish = true
				mutex.Unlock()
				wg.Wait()
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}

}

// callForHeartbeat
/*
	想Coordinator发送完成任务的RPC
*/
func callForHeartbeat(heartbeat *Heartbeat, reply *HeartbeatReply) error {
	ok := call("Coordinator.SolveHearBeat", &heartbeat, &reply)
	if !ok {
		return errors.New("no reply")
	}
	return nil
}

// CallForFinish
/*
	想Coordinator发送完成任务的RPC
*/
func callForFinish(workFinish *WorkFinish, reply *WorkFinishReply) error {
	ok := call("Coordinator.ReceiveFinish", &workFinish, &reply)
	if !ok {
		return errors.New("no reply")
	}
	return nil
}

// CallForTask
/*
	想Coordinator发送请求任务的RPC
*/
func CallForTask(requestInfo *RequestTask, taskInfo *TaskInfo) error {
	ok := call("Coordinator.AllocateTask", &requestInfo, &taskInfo)
	if !ok {
		return errors.New("no reply")
	}
	return nil
}

// call
/*
	用于给Coordinate发送RPC请求
*/
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
