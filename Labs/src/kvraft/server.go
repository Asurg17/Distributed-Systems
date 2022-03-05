package kvraft

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string
	OpKey   string
	OpValue string
	OpCliId int64
	OpReqId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storedData         map[string]string
	executedOperations map[int64]int
	commitedOpIndexes  map[int]string
}

type AppendReply struct {
	err   Err
	value string // for get operation
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.OpType = "Get"
	op.OpKey = args.Key
	op.OpValue = ""
	op.OpCliId = args.CliId
	op.OpReqId = args.ReqId

	appendReply := AppendReply{}

	kv.appendOpToRaftLog(op, &appendReply)

	reply.Err = appendReply.err
	reply.Value = appendReply.value

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{}
	op.OpType = args.Op
	op.OpKey = args.Key
	op.OpValue = args.Value
	op.OpCliId = args.CliId
	op.OpReqId = args.ReqId

	appendReply := AppendReply{}

	kv.appendOpToRaftLog(op, &appendReply)

	reply.Err = appendReply.err

}

func (kv *KVServer) appendOpToRaftLog(op Op, reply *AppendReply) {

	commandIndex, _, isServerLeader := kv.rf.Start(op)

	if !isServerLeader {
		reply.err = ErrWrongLeader
		return
	}

	time.Sleep(WaitTime * time.Millisecond) // wait for Raft to complete agreement

	kv.checkIfCommited(commandIndex, op, reply)

}

func (kv *KVServer) checkIfCommited(commandIndex int, op Op, reply *AppendReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if opIdentifier, ok := kv.commitedOpIndexes[commandIndex]; ok {

		currIdentifier := strconv.FormatInt(op.OpCliId, 10) + ":" + strconv.Itoa(op.OpReqId)

		if opIdentifier == currIdentifier {
			reply.err = OK
			reply.value = kv.storedData[op.OpKey]
		}

	}

}

func (kv *KVServer) ChanListener() {

	for {

		applyMsg := <-kv.applyCh

		kv.mu.Lock()

		if applyMsg.Snapshot != nil {

			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := labgob.NewDecoder(r)

			var LastIncludedIndex int
			var LastIncludedTerm int
			d.Decode(&LastIncludedIndex)
			d.Decode(&LastIncludedTerm)
			d.Decode(&kv.storedData)
			d.Decode(&kv.executedOperations)

		} else {

			command := applyMsg.Command
			commandIndex := applyMsg.CommandIndex

			op := command.(Op)

			requestId, ok := kv.executedOperations[op.OpCliId]

			if (ok && requestId < op.OpReqId) || !ok { // if this operation was not executed already

				kv.executedOperations[op.OpCliId] = op.OpReqId

				if op.OpType == "Get" {
					// do nothing?
				} else if op.OpType == "Put" {
					kv.storedData[op.OpKey] = op.OpValue
				} else if op.OpType == "Append" {
					if value, ok := kv.storedData[op.OpKey]; ok {
						kv.storedData[op.OpKey] = value + op.OpValue
					} else { // works like put if such key does not exist
						kv.storedData[op.OpKey] = op.OpValue
					}
				}
			}

			kv.commitedOpIndexes[commandIndex] = strconv.FormatInt(op.OpCliId, 10) + ":" + strconv.Itoa(op.OpReqId)

			if kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.storedData)
				e.Encode(kv.executedOperations)
				go kv.rf.Snapshot(w.Bytes(), applyMsg.CommandIndex)
			}

		}

		kv.mu.Unlock()

	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.storedData = make(map[string]string)
	kv.executedOperations = make(map[int64]int)
	kv.commitedOpIndexes = make(map[int]string)

	go kv.ChanListener()

	return kv
}
