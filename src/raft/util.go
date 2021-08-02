package raft

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

func (rf *Raft) getTimeout() time.Duration {
	min := 200
	max := 400
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
	var success uint32
	wg := sync.WaitGroup{}
	rf.mu.Lock()
	lastLogIndex := len(rf.logEntries) - 1
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		go func(i int) {
			rf.mu.Lock()
			//这里可能为-1，index从0开始
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: prevLogIndex,
				LeaderCommit: commitIndex,
			}
			//存在上一个log entry
			if prevLogIndex >= 0 {
				args.Term = rf.logEntries[prevLogIndex].Term
			}
			//要发送给follower的log
			if lastLogIndex >= rf.nextIndex[i] {
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
			defer func() {
				rf.mu.Unlock()
				wg.Done()
			}()

			//判断是不是过时的leader
			if reply.Term > rf.currentTerm {
				rf.resetToFollower()
				return
			}

			if len(args.LogEntries) == 0 {
				return
			}

			//发送日志成功
			if reply.Success {
				atomic.AddUint32(&success, 1)
				rf.nextIndex[i] = prevLogIndex + 1 + len(args.LogEntries)
				rf.matchIndex[i] = prevLogIndex + len(args.LogEntries)
				return
			}

			if reply.Xterm == -1 {
				rf.nextIndex[i] = reply.XLen
				return
			}

			index := -1
			for i, v := range rf.logEntries {
				if v.Term == reply.Xterm {
					index = i
				}
			}
			if index == -1 {
				rf.nextIndex[i] = reply.XIndex
			} else {
				rf.nextIndex[i] = index
			}
		}(i)
	}

	wg.Wait()
	rf.commitMajority(commitIndex, lastLogIndex)
}

func (rf *Raft) sendRequestVoteRPCToOthers() (uint32, int) {
	//leader自投一票
	var success uint32 = 1
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
			log.Printf("%d 向 %d 发起选举 ok: %t, 请求:%+v, 回复:%+v\n", rf.me, i, ok, args, reply)
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
	atomic.StoreUint32(&rf.isHeartbeatTickerLive, 0)
	rf.serverState = Follower
	rf.votedFor = -1
}

func (rf *Raft) isMajority(success uint32) bool {
	return success >= uint32((len(rf.peers)/2)+1)
}

//检测某个index位置是否有多于一半的peers，如果是，标记为commit
func (rf *Raft) commitMajority(commitIndex, lastLogIndex int) {
	for i := commitIndex + 1; i <= lastLogIndex; i++ {
		var success uint32
		for peer := range rf.peers {
			if rf.matchIndex[peer] >= i && rf.logEntries[i].Term == rf.currentTerm {
				success++
			}
		}

		if rf.isMajority(success) {
			rf.mu.Lock()
			for j := rf.commitIndex + 1; j <= i; j++ {
				log.Printf("%d apply msg: %+v\n", rf.me, rf.logEntries[j])
				rf.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[j].Command,
					CommandIndex: rf.logEntries[j].Index + 1,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
			rf.mu.Unlock()
		}
	}
}
