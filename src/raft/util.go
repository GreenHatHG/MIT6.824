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

func (rf *Raft) applyLogs() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{true, rf.logEntries[i].Command, i}
		rf.applyMsg <- msg
		rf.Info("apply msg: %+v\n", msg)
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) checkMatchIndexMajority() {
	for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
		matchIndexNum := 0
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= i {
				matchIndexNum++
			}
			if !rf.isMajority(matchIndexNum) || rf.logEntries[i].Term != rf.currentTerm {
				continue
			}
			rf.commitIndex = i
			rf.applyLogs()
			break
		}
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
