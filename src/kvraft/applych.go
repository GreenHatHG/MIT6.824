package kvraft

import "fmt"

func (kv *KVServer) applyChLoop() {
	for ch := range kv.applyCh {
		op := ch.Command.(Op)
		kv.mu.Lock()
		fmt.Println(kv.me)
		a, b := kv.rf.GetState()
		fmt.Println(kv.me, a, b)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.doPutAppend(op)
		} else {
			fmt.Println(kv.me, "kv.LeaderCommitCallback <- op")
			kv.LeaderCommitCallback <- op
		}
		kv.mu.Unlock()
	}
}
