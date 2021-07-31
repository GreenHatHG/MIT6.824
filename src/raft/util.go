package raft

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

func (rf *Raft) getTimeout() time.Duration {
	min := 450
	max := 600
	timeout := rf.newRand.Intn(max-min) + min
	return time.Duration(timeout) * time.Millisecond
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntriesToOthers() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			//这里可能为-1
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: prevLogIndex,
			}
			//存在上一个log entry
			if prevLogIndex >= 0 {
				args.Term = rf.logEntries[prevLogIndex].Term
			}
			//leader收到新的command
			if len(rf.logEntries)-1 >= rf.nextIndex[i] {
				args.LogEntries = rf.logEntries[rf.nextIndex[i]:]
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}

			//是否发送成功
			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			log.Printf("%d 发送给 %d 的内容:%+v, 回复:%+v\n", rf.me, i, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			//判断是不是过时的leader
			if reply.Term > rf.currentTerm {
				rf.heartbeatsTicker.Stop()
				rf.serverState = Follower
				return
			}

			if reply.Success {
				rf.nextIndex[i] = prevLogIndex + 1 + len(args.LogEntries)
				rf.sendApplyMsg(args.LogEntries)
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendApplyMsg(logEntries []LogEntry) {
	for _, logEntry := range logEntries {
		rf.applyMsg <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
	}
}

func (rf *Raft) sendRequestVoteRPCToOthers() (uint32, int) {
	var success uint32
	var maxTerm int
	wg := sync.WaitGroup{}
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	maxTermLocker := sync.Mutex{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reply := &RequestVoteReply{}
			log.Println(rf.me, "向", i, "发起选举")
			ok := rf.peers[i].Call("Raft.RequestVote", args, reply)
			log.Println(rf.me, "向", i, "发起选举，结果：", "ok:", ok, "VoteGranted:", reply.VoteGranted)
			if ok && reply.VoteGranted {
				atomic.AddUint32(&success, 1)
			}
			maxTermLocker.Lock()
			maxTerm = int(math.Max(float64(maxTerm), float64(reply.Term)))
			maxTermLocker.Unlock()
		}(i)
	}
	wg.Wait()
	return success, maxTerm
}

func (rf *Raft) resetToFollower() {
	rf.serverState = Follower
	rf.votedFor = -1
}
