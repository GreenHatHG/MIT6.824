package raft

func (rf *Raft) becomeFollower() {
	rf.serverState = Follower
	rf.timeoutInterval = 0
	rf.votedFor = -1
	rf.tick = rf.electTicker
}

func (rf *Raft) becomeLeader() {
	rf.raftLog.Println("---------------转变为leader")
	rf.serverState = Leader
	rf.timeoutInterval = 2
	rf.votedFor = -1
	rf.tick = rf.heartBeatTicker
	go rf.heartBeatTicker()
}

func (rf *Raft) becomeCandidate() {
	rf.raftLog.Println("---------------转变为candidate")
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.timeoutInterval = 0
}
