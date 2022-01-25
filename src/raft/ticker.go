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
	rf.Log("选举定时器到时，转变为Candidate，发起选举\n")
	rf.becomeCandidate()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	majority, maxTerm := rf.requestVoteRPC(currentTerm)
	rf.Log("请求投票返回，majority:%t maxTerm:%d", majority, maxTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != Candidate {
		rf.Log("不是Candidate，退出选举，当前状态为[%d]\n", rf.serverState)
		return
	}

	if maxTerm > rf.currentTerm {
		rf.currentTerm = maxTerm
		rf.becomeFollower()
		rf.Log("选举失败，存在更大Term，rf.currentTerm更新为[%d]\n", maxTerm)
		return
	}

	if majority {
		rf.Log("-----------------------选举成功\n")
		rf.becomeLeader()
	}
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
	maxTerm := rf.appendEntriesRPC()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.serverState != Leader {
		return
	}
	if maxTerm > rf.currentTerm {
		rf.currentTerm = maxTerm
		rf.becomeFollower()
		rf.Log("心跳结束后转变为follower，存在更大Term，rf.currentTerm更新为[%d]\n", maxTerm)
	}
}
