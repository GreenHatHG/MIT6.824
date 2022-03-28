package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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

	// You will have to modify this function.
	args := GetArgs{Key: key}
	for {
		for i := range ck.servers {
			reply := GetReply{}
			DPrintf("Clerk向[%d]发送Get请求", i)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			DPrintf("Clerk向[%d]请求Get返回，请求:%+v, 回复:%+v", i, args, reply)
			if ok && reply.Err == OK {
				return reply.Value
			}
			if ok && reply.Err == ErrNoKey {
				return ""
			}
		}
		time.Sleep(1 * time.Second)
	}
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
	args := PutAppendArgs{Key: key, Value: value, Op: op}
	for {
		for i := range ck.servers {
			reply := GetReply{}
			DPrintf("Clerk向[%d]发送%s请求", i, op)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			DPrintf("Clerk向[%d]请求%s返回，请求:%+v, 回复:%+v", i, op, args, reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
