package kvraft

import "sync/atomic"

func (kv *KVServer) applyChLoop() {
	for ch := range kv.applyCh {
		op := ch.Command.(Op)
		DPrintf("[KVServer %d]接收到applyCh:%+v", kv.me, ch)

		if ch.RaftTerm > atomic.LoadInt64(&kv.LeaderTerm) {
			atomic.StoreInt64(&kv.LeaderTerm, ch.RaftTerm)
		}

		kv.mu.Lock()
		//Get的日志，跳过
		if op.KVOpType != "Get" {
			kv.doPutAppend(op)
		}
		if ch, ok := kv.CallbackSub[op.RequestId]; ok {
			ch <- op
			delete(kv.CallbackSub, op.RequestId)
		}
		kv.mu.Unlock()
	}
}
