package raft

func (rf *Raft) isMajority(success int) bool {
	majority := success >= (len(rf.peers)/2)+1
	return majority
}
