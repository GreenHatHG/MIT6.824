package raft

import (
	"fmt"
	"log"
	"os"
)

var (
	Info *log.Logger
	Warn *log.Logger
)

func init() {
	Info = log.New(os.Stdout, "Info:", log.LstdFlags|log.Lmicroseconds)
	Warn = log.New(os.Stdout, "Warn:", log.LstdFlags|log.Lmicroseconds)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
