package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
	me             int64
	lastRequestId  int
	leaderServerId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// You'll have to add code here.
	ck.me = nrand()
	ck.lastRequestId = 0
	ck.leaderServerId = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{}
	args.Key = key
	args.CliId = ck.me
	args.ReqId = ck.lastRequestId

	ck.lastRequestId++

	serverIndex := ck.leaderServerId
	returnValue := ""

	num_of_servers := len(ck.servers)

	for {

		serverIndex = (serverIndex) % num_of_servers

		reply := GetReply{}
		ok := ck.servers[serverIndex].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err == OK {
			returnValue = reply.Value
			ck.leaderServerId = serverIndex
			break
		} else if reply.Err == ErrNoKey {
			ck.leaderServerId = serverIndex
			break
		}

		serverIndex++

	}

	// You will have to modify this function.
	return returnValue
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{}

	args.Op = op
	args.Key = key
	args.Value = value
	args.CliId = ck.me
	args.ReqId = ck.lastRequestId

	ck.lastRequestId++

	serverIndex := ck.leaderServerId

	num_of_servers := len(ck.servers)

	for {

		serverIndex = (serverIndex) % num_of_servers

		reply := PutAppendReply{}
		ok := ck.servers[serverIndex].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			ck.leaderServerId = serverIndex
			break
		}

		serverIndex++

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
