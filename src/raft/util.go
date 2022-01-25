package raft

import (
	"fmt"
	"log"
)

// Debugging
//const Debug = 0
//
//func DPrintf(format string, a ...interface{}) {
//	if Debug > 0 {
//		log.Printf(format, a...)
//	}
//}

const Prefix = "【%d】 term:%3d state:%s | "

func (rf *Raft) Log(format string, a ...interface{}) {
	prefix := fmt.Sprintf(Prefix, rf.me, rf.currentTerm, StateString(rf.serverState))
	log.Printf(prefix+format, a...)
}

func (rf *Raft) isMajority(success int) bool {
	majority := success >= (len(rf.peers)/2)+1
	return majority
}

func (rf *Raft) applyLogs() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{true, rf.logEntries[i].Command, i}
		rf.applyMsg <- msg
		rf.Log("apply msg: %+v\n", msg)
	}
	rf.lastApplied = rf.commitIndex
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
