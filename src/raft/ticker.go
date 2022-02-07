package raft

import "time"

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)
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
	rf.mu.Unlock()

	rf.requestVoteRPC()
}

func (rf *Raft) heartBeatTicker() {
	rf.mu.Lock()
	//rf.raftLog.Println("heartBeatTicker, 当前timeoutInterval:", rf.timeoutInterval)
	if rf.timeoutInterval < 2 || rf.serverState != Leader {
		rf.mu.Unlock()
		return
	}
	rf.timeoutInterval = 0
	rf.mu.Unlock()
	rf.appendEntriesRPC()
}
