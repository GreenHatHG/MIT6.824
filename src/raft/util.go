package raft

import (
	"fmt"
	"log"
	"os"
)

var (
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
)

func init() {
	Info = log.New(os.Stdout, "Info:", log.LstdFlags|log.Lmicroseconds)
	Warn = log.New(os.Stdout, "Warn:", log.LstdFlags|log.Lmicroseconds)
	Error = log.New(os.Stdout, "Error:", log.LstdFlags|log.Lmicroseconds)
}

const Prefix = "【%d】 term:%3d state:%s | "

func (rf *Raft) Info(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf(Prefix, rf.me, rf.currentTerm, StateString(rf.serverState))
		Info.Printf(prefix+format, a...)
	}
}

func (rf *Raft) Warn(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf(Prefix, rf.me, rf.currentTerm, StateString(rf.serverState))
		Warn.Printf(prefix+format, a...)
	}
}

func (rf *Raft) Error(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf(Prefix, rf.me, rf.currentTerm, StateString(rf.serverState))
		Error.Printf(prefix+format, a...)
	}
}

func (rf *Raft) isMajority(success int) bool {
	majority := success >= (len(rf.peers)/2)+1
	return majority
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) resetTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) leaderMaybeCommit() {
	for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
		if rf.logEntries[i].Term != rf.currentTerm {
			continue
		}

		matchIndexNum := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= i {
				matchIndexNum++
			}
		}
		if !rf.isMajority(matchIndexNum) {
			return
		}
		rf.commitIndex = i
	}
	if rf.commitIndex > rf.lastApplied {
		rf.applyCond.Broadcast()
	}
}
