package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

const Debug = 1

var (
	Info *log.Logger
	Warn *log.Logger
)

func init() {
	Info = log.New(os.Stdout, "Info:", log.LstdFlags|log.Lmicroseconds)
	Warn = log.New(os.Stdout, "Warn:", log.LstdFlags|log.Lmicroseconds)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		Info.Printf(format, a...)
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	KVOpType string
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data                 map[string]string
	HasCommitted         map[string]struct{}
	LeaderCommitCallback chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = OK

	term, isLeader := kv.rf.GetState()
	DPrintf("---------------debug: [KVServer %d] isLeader:%v, term:%d", kv.me, isLeader, term)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[KVServer %d] isLeader:%v, term:%d", kv.me, isLeader, term)
	if value, ok := kv.Data[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Value = value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer func() {
		if reply.Err == OK {
			kv.HasCommitted[args.ArgsId] = struct{}{}
		}
		kv.mu.Unlock()
	}()
	reply.Err = OK

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[KVServer %d] isLeader:%v, term:%d", kv.me, isLeader, term)
	if _, ok := kv.HasCommitted[args.ArgsId]; ok {
		DPrintf("[KVServer %d]已经处理过: %+v ", kv.me, args)
		return
	}

	op := Op{
		KVOpType: args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	kv.rf.Start(op)
	fmt.Println(kv.me, "kv.doPutAppend(<-kv.LeaderCommitCallback)")
	kv.doPutAppend(<-kv.LeaderCommitCallback)
}

func (kv *KVServer) doPutAppend(op Op) {
	DPrintf("[KVServer %d] 执行%+v", kv.me, op)
	if op.KVOpType == "Put" {
		kv.Data[op.Key] = op.Value
	} else if op.KVOpType == "Append" {
		oldValue, ok := kv.Data[op.Key]
		if !ok {
			kv.Data[op.Key] = op.Value
			return
		}
		newValue := oldValue + op.Value
		kv.Data[op.Key] = newValue
		op.Value = newValue
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
	Warn.Printf("-----------------[KVServer %d] kill", kv.me)
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

	// You may need initialization code here.
	kv.Data = make(map[string]string)
	kv.HasCommitted = make(map[string]struct{})
	kv.LeaderCommitCallback = make(chan Op)
	go kv.applyChLoop()
	return kv
}
