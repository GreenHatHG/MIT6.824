package raft

import "time"

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(25 * time.Millisecond)
		rf.mu.Lock()
		rf.timeoutInterval++
		go rf.tick()
		rf.mu.Unlock()
	}
}

func (rf *Raft) electTicker() {
	rf.mu.Lock()
	interval := rf.getRandomInterval()
	//rf.raftLog.Println("electTicker, 获取的interval:", interval, "当前timeoutInterval:", rf.timeoutInterval)
	if rf.timeoutInterval < interval || rf.serverState == Leader {
		rf.mu.Unlock()
		return
	}
	rf.Info("选举定时器到时，转变为Candidate，发起选举\n")
	rf.becomeCandidate()
	rf.requestVoteRPC()
	rf.mu.Unlock()
}

func (rf *Raft) heartBeatTicker() {
	rf.mu.Lock()
	if rf.timeoutInterval < 4 || rf.serverState != Leader {
		rf.mu.Unlock()
		return
	}
	rf.timeoutInterval = 0
	rf.appendEntriesRPC()
	rf.mu.Unlock()
}

func (rf *Raft) checkCommitLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		commitEntries := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{true, rf.logEntries[i].Command, i}
			commitEntries = append(commitEntries, msg)
		}
		rf.mu.Unlock()

		for _, msg := range commitEntries {
			rf.applyMsg <- msg
			rf.mu.Lock()
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
		rf.Info("apply msg: %+v\n", commitEntries)
	}
}
