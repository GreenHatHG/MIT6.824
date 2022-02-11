package raft

func (rf *Raft) becomeFollower(resetTimeout, resetVotedFor bool) {
	rf.serverState = Follower
	if resetTimeout {
		rf.timeoutInterval = 0
	}
	if resetVotedFor {
		rf.votedFor = -1
		rf.persist()
	}
	rf.tick = rf.electTicker
}

func (rf *Raft) becomeLeader() {
	rf.serverState = Leader
	rf.timeoutInterval = 0
	rf.tick = rf.heartBeatTicker
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logEntries)
	}
	rf.Warn("---------------转变为leader\n")
}

func (rf *Raft) becomeCandidate() {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.timeoutInterval = 0
	rf.persist()
	rf.Warn("---------------转变为candidate\n")
}
