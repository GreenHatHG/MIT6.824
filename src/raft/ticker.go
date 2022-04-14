package raft

import (
	"time"
)

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		if rf.serverState == Leader {
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.resetElectionTimerTime) >= rf.electionTimeout {
			rf.Info("选举定时器到时，转变为Candidate，发起选举\n")
			rf.electionTimeout = rf.getRandomElectionTimeout()
			rf.serverState = Candidate
			rf.resetTerm(rf.currentTerm + 1)
			rf.votedFor = rf.me
			rf.requestVoteRPC()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeatTicker() {
	for !rf.killed() {
		time.Sleep(heartBeatInterval)
		rf.mu.Lock()
		if rf.serverState != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.appendEntriesRPC(false)
		rf.mu.Unlock()
	}
}

func (rf *Raft) doApplyLog() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for !rf.killed() {
		for !rf.needApply() {
			rf.applyCond.Wait()
		}

		rf.mu.Lock()
		i := rf.lastApplied + 1
		msg := ApplyMsg{true, rf.logEntries[i].Command, i, int64(rf.currentTerm)}
		rf.Info("apply msg: %+v\n", msg)
		rf.mu.Unlock()

		rf.applyMsg <- msg

		rf.mu.Lock()
		rf.lastApplied = i
		rf.mu.Unlock()
	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex > rf.lastApplied
}
