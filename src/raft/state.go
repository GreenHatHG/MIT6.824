package raft

func (rf *Raft) becomeFollower(resetTimeout, resetVotedFor bool, newTerm int) {
	rf.serverState = Follower
	if resetTimeout {
		rf.resetTimer(false)
	}
	if resetVotedFor {
		rf.votedFor = -1
	}
	if newTerm != -1 {
		rf.currentTerm = newTerm
	}
	if resetVotedFor || newTerm != -1 {
		rf.persist()
	}
	rf.tick = rf.electTicker
}

func (rf *Raft) becomeLeader() {
	rf.serverState = Leader
	rf.resetTimer(true)
	rf.tick = rf.heartBeatTicker
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logEntries)
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	rf.appendEntriesRPC()
	rf.Warn("---------------转变为leader\n")
}

func (rf *Raft) becomeCandidate() {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetTimer(false)
	rf.persist()
	rf.Warn("---------------转变为candidate\n")
}
