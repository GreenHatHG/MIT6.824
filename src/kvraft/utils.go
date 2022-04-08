package kvraft

import "time"

func (ck *Clerk) replicateServiceClient(callOne func(int, chan interface{})) interface{} {
	ck.mu.Lock()
	prefer := ck.leaderPrefer
	ck.mu.Unlock()

	numServer := len(ck.servers)
	done := make(chan interface{})
	const timeout = 1 * time.Second
	t := time.NewTimer(timeout)

	for {
		for i := range ck.servers {
			id := (prefer + i) % numServer

			go callOne(id, done)

			select {
			case r := <-done:
				ck.mu.Lock()
				ck.leaderPrefer = id
				ck.mu.Unlock()
				return r
			case <-t.C:
				t.Reset(timeout)
			}
		}
	}
}

func (ck *Clerk) getRPC(args *GetArgs) *GetReply {
	mainFunc := func(i int, done chan interface{}) {
		reply := &GetReply{}
		DPrintf("Clerk Get: 向[%d]发送请求:%+v", i, args)
		ok := ck.servers[i].Call("KVServer.Get", args, reply)
		DPrintf("Clerk Get:[%d]返回，ok: %v, 请求:%+v, 回复:%+v", i, ok, args, reply)
		if ok && reply.Err != ErrWrongLeader {
			select {
			case done <- reply:
			default:
			}
		}
	}
	return ck.replicateServiceClient(mainFunc).(*GetReply)
}

func (ck *Clerk) putAppendRPC(args *PutAppendArgs, op string) *PutAppendReply {
	mainFunc := func(i int, done chan interface{}) {
		reply := &PutAppendReply{}
		DPrintf("Clerk %s: 向[%d]发送请求:%+v", op, i, args)
		ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
		DPrintf("Clerk %s: [%d]返回，ok: %v, 请求:%+v, 回复:%+v", op, i, ok, args, reply)
		if ok && reply.Err == OK {
			select {
			case done <- reply:
			default:
			}
		}
	}
	return ck.replicateServiceClient(mainFunc).(*PutAppendReply)
}
