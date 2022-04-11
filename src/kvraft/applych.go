package kvraft

func (kv *KVServer) applyChLoop() {
	for ch := range kv.applyCh {
		op := ch.Command.(Op)
		DPrintf("KVServer [%d]接收到applyCh:%+v", kv.me, op)
		_, isLeader := kv.rf.GetState()

		if !isLeader {
			kv.mu.Lock()
			kv.doPutAppend(op)
			kv.mu.Unlock()
		} else {
			kv.LeaderCommitCallback <- op
		}
	}
}
