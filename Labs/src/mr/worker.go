package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var FILE_PREFIX = "/mr"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func runMapper(filename string, taskNumber int, mapf func(string, string) []KeyValue) {

	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	numOfTasks := getNumOfTasks()

	temporaryFileElems := map[string][]KeyValue{}

	directory, _ := os.Getwd()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		curReduceNum := ihash(intermediate[i].Key) % numOfTasks.NumOfReduceTasks
		tempFileName := "temp-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(curReduceNum)

		for k := i; k < j; k++ {
			temporaryFileElems[tempFileName] = append(temporaryFileElems[tempFileName], intermediate[k])
		}

		i = j
	}

	for filename, keyvalueArray := range temporaryFileElems {

		osFile, _ := ioutil.TempFile(directory, filename)

		file, _ := os.OpenFile(osFile.Name(), os.O_RDWR, os.ModePerm)

		enc := json.NewEncoder(file)

		for _, curKeyValue := range keyvalueArray {
			enc.Encode(&curKeyValue)
		}

		file.Close()

		finalFilePath := directory + FILE_PREFIX + filename[4:] + ".json"

		os.Rename(osFile.Name(), finalFilePath)

	}

	args := TaskCompleteRequestArgs{}
	response := EmptyResponseArgs{}

	args.TaskNumber = taskNumber

	call("Master.HandleTaskCompleteRequest", &args, &response)

}

func runReducer(taskNumber int, reducef func(string, []string) string) {

	directory, _ := os.Getwd()
	numOfTasks := getNumOfTasks()

	intermediate := []KeyValue{}

	for i := 0; i < numOfTasks.NumOfMapTasks; i++ {

		currentFileName := directory + FILE_PREFIX + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNumber) + ".json"

		file, _ := os.OpenFile(currentFileName, os.O_RDWR, os.ModePerm)

		dec := json.NewDecoder(file)

		for {

			var kv KeyValue

			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)

		}

		file.Close()

	}

	sort.Sort(ByKey(intermediate))

	tempFileName := "temp-out-" + strconv.Itoa(taskNumber)
	tempFile, _ := ioutil.TempFile(directory, tempFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	realFilename := directory + FILE_PREFIX + "-out-" + strconv.Itoa(taskNumber)
	os.Rename(tempFile.Name(), realFilename)

	args := TaskCompleteRequestArgs{}
	response := EmptyResponseArgs{}

	args.TaskNumber = taskNumber

	call("Master.HandleTaskCompleteRequest", &args, &response)

}

func getNumOfTasks() ResponseNumOfTasks {

	args := EmptyRequestArgs{}
	response := ResponseNumOfTasks{}

	call("Master.GetNumOfTasks", &args, &response)

	return response

}

func requestTask() (ResponseTaskArgs, bool) {

	args := EmptyRequestArgs{}
	response := ResponseTaskArgs{}

	if !call("Master.HandleTaskRequest", &args, &response) {
		return response, false
	}

	return response, true

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {

		// sleep some time before each request
		time.Sleep(500 * time.Millisecond)

		response, ok := requestTask()

		if !ok {
			break
		}

		if response.TaskType == "map" {

			runMapper(response.Filename, response.TaskNumber, mapf)

		} else if response.TaskType == "reduce" {

			runReducer(response.TaskNumber, reducef)

		}

	}

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
