package raft

import (
	"math"
	"sync"
	"time"
)

type requestVoteRes struct {
	majority bool
	maxTerm  int
}

func (rf *Raft) requestVoteRPC(currentTerm int) (bool, int) {
	//本身自投一票
	success, maxTerm := 1, 0

	resC := make(chan requestVoteRes, len(rf.peers)-1)
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logEntries) - 1,
		LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		server := server
		if server == rf.me {
			continue
		}
		go func() {
			reply := &RequestVoteReply{}

			t := time.Now()
			rf.Info("开始向[%d]索要投票\n", server)

			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.Info("向[%d]索要投票的结果，发送时间: %v, ok: %t, 请求:%+v, 回复:%+v\n", server, t, ok, args, reply)
			if !ok {
				return
			}

			if reply.VoteGranted {
				success++
			}
			maxTerm = int(math.Max(float64(maxTerm), float64(reply.Term)))
			if rf.isMajority(success) {
				resC <- requestVoteRes{true, maxTerm}
			}
		}()
	}
	res := <-resC
	return res.majority, res.maxTerm
}

func (rf *Raft) appendEntriesRPC() int {
	term := make(chan int, len(rf.peers)-1)
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	numCommit := 1
	hasCommitted := false

	go func() {
		//没有最大的term大于leader的，返回-1，不会被任何处理
		wg.Wait()
		term <- -1
	}()

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		i := i
		if i == rf.me {
			continue
		}

		go func() {
			defer wg.Done()
			rf.mu.Lock()
			if rf.serverState != Leader {
				rf.mu.Unlock()
				return
			}
			lastLogIndex := len(rf.logEntries) - 1
			nextIndex := rf.nextIndex[i]
			prevLogIndex := nextIndex - 1
			commitIndex := rf.commitIndex
			args := &AppendEntriesArgs{
				Term:         currentTerm,
				PrevLogIndex: prevLogIndex,
				LeaderCommit: commitIndex,
				PrevLogTerm:  rf.logEntries[prevLogIndex].Term,
			}
			reply := &AppendEntriesReply{}
			//要发送给follower的log
			if lastLogIndex >= nextIndex {
				args.LogEntries = rf.logEntries[nextIndex:]
			}
			rf.mu.Unlock()

			t := time.Now()
			rf.Info("开始向[%d]发送心跳\n", i)
			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

			rf.mu.Lock()
			rf.Info("向[%d]发送appendEntriesRPC返回，发送时间：%v, ok: %t, 请求:%+v, 回复:%+v\n", i, t, ok, args, reply)
			rf.mu.Unlock()

			if !ok {
				return
			}
			//leader过期
			if reply.Term > currentTerm {
				term <- reply.Term
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.serverState != Leader {
				return
			}
			//发送日志成功
			if reply.Success {
				numCommit++
				if !hasCommitted && numCommit > len(rf.peers)/2 {
					hasCommitted = true
					rf.commitIndex = prevLogIndex + len(args.LogEntries)
					rf.applyLogs()
				}
				rf.nextIndex[i] = prevLogIndex + len(args.LogEntries) + 1
				rf.matchIndex[i] = prevLogIndex + len(args.LogEntries)
				return
			}
			//如果leader没有conflictTerm的日志，那么重新发送所有日志
			rf.nextIndex[i] = 1
			for j := reply.ConflictIndex; j > 0; j-- {
				if rf.logEntries[j].Term == reply.ConflictTerm {
					rf.nextIndex[j] = j + 1
					break
				}
			}
		}()
	}
	return <-term
}
