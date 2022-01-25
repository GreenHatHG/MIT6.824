package raft

func (rf *Raft) becomeFollower() {
	rf.serverState = Follower
	rf.timeoutInterval = 0
	rf.votedFor = -1
	rf.tick = rf.electTicker
}

func (rf *Raft) becomeLeader() {
	rf.Warn("---------------转变为leader\n")
	rf.serverState = Leader
	rf.timeoutInterval = 2
	rf.votedFor = -1
	rf.tick = rf.heartBeatTicker
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logEntries)
	}
	go rf.heartBeatTicker()
}

func (rf *Raft) becomeCandidate() {
	rf.Warn("---------------转变为candidate\n")
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.timeoutInterval = 0
}
