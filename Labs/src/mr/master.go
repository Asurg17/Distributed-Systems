package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mainLock sync.Mutex

type Master struct {
	// Your definitions here.
	numOfReduceTasks     int
	numOfReduceTasksLeft int
	numOfMapTasks        int
	numOfMapTasksLeft    int
	mapTasks             []Task
	reduceTasks          []Task
	mapTasksStatus       map[int]bool
	reduceTasksStatus    map[int]bool
	files                []string
}

type Task struct {
	filename             string
	alreadyProcessedFlag bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) HandleTaskRequest(args *EmptyRequestArgs, response *ResponseTaskArgs) error {

	mainLock.Lock()
	defer mainLock.Unlock()

	if m.numOfMapTasksLeft > 0 {

		for index, mapTask := range m.mapTasks {

			if !mapTask.alreadyProcessedFlag {

				response.TaskNumber = index
				response.Filename = mapTask.filename
				response.TaskType = "map"

				m.mapTasks[index].alreadyProcessedFlag = true

				go func(taskNumber int) {

					//sleep 10 seconds and then check if task has finished
					time.Sleep(10 * time.Second)

					mainLock.Lock()
					defer mainLock.Unlock()

					// if did not finish, re-issue
					if !m.mapTasksStatus[taskNumber] {
						m.mapTasks[taskNumber].alreadyProcessedFlag = false
					}

				}(index)

				return nil
			}

		}

	} else if m.numOfReduceTasksLeft > 0 {

		for index, reduceTask := range m.reduceTasks {

			if !reduceTask.alreadyProcessedFlag {

				response.TaskNumber = index
				response.Filename = ""
				response.TaskType = "reduce"

				m.reduceTasks[index].alreadyProcessedFlag = true

				go func(taskNumber int) {

					//sleep 10 seconds and then check if task has finished
					time.Sleep(10 * time.Second)

					mainLock.Lock()
					defer mainLock.Unlock()

					// if did not finish, re-issue
					if !m.reduceTasksStatus[taskNumber] {
						m.reduceTasks[taskNumber].alreadyProcessedFlag = false
					}

				}(index)

				return nil
			}

		}

	}

	response.TaskType = "no task"

	return nil
}

func (m *Master) HandleTaskCompleteRequest(args *TaskCompleteRequestArgs, response *EmptyResponseArgs) error {

	mainLock.Lock()
	defer mainLock.Unlock()

	if m.numOfMapTasksLeft > 0 {
		m.numOfMapTasksLeft--
		m.mapTasksStatus[args.TaskNumber] = true
	} else {
		m.numOfReduceTasksLeft--
		m.reduceTasksStatus[args.TaskNumber] = true

		// Empty already processed json files
		directory, _ := os.Getwd()
		for i := 0; i < m.numOfMapTasks; i++ {

			currentFileName := directory + FILE_PREFIX + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(args.TaskNumber) + ".json"
			os.Truncate(currentFileName, 0)

		}

	}

	return nil
}

func (m *Master) GetNumOfTasks(args *EmptyRequestArgs, response *ResponseNumOfTasks) error {

	response.NumOfMapTasks = m.numOfMapTasks
	response.NumOfReduceTasks = m.numOfReduceTasks

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	mainLock.Lock()
	defer mainLock.Unlock()

	if m.numOfMapTasksLeft == 0 && m.numOfReduceTasksLeft == 0 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.numOfReduceTasks = nReduce
	m.numOfReduceTasksLeft = nReduce

	m.numOfMapTasks = len(files)
	m.numOfMapTasksLeft = len(files)

	m.mapTasks = []Task{}
	m.reduceTasks = []Task{}

	m.mapTasksStatus = make(map[int]bool)
	m.reduceTasksStatus = make(map[int]bool)

	m.files = files

	// Create map tasks
	for index, file := range files {
		mapTask := Task{file, false}
		m.mapTasks = append(m.mapTasks, mapTask)
		m.mapTasksStatus[index] = false
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{"", false}
		m.reduceTasks = append(m.reduceTasks, reduceTask)
		m.reduceTasksStatus[i] = false
	}

	m.server()
	return &m
}
