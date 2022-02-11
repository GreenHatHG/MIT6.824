package raft

import (
	"time"
)

func (rf *Raft) requestVoteRPC() {
	//本身自投一票
	success := 1

	for server := range rf.peers {
		server := server
		if server == rf.me {
			continue
		}
		go func() {
			reply := &RequestVoteReply{}

			rf.mu.Lock()
			if rf.serverState != Candidate {
				rf.mu.Unlock()
				return
			}

			t := time.Now()
			currentTerm := rf.currentTerm
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logEntries) - 1,
				LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
			}
			rf.Info("开始向[%d]索要投票\n", server)
			rf.mu.Unlock()

			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.Info("向[%d]索要投票的结果，发送时间: %v, ok: %t, 请求:%+v, 回复:%+v\n", server, t, ok, args, reply)
			if !ok {
				return
			}
			if rf.serverState != Candidate {
				rf.Info("不是Candidate，退出选举，当前状态为[%d]\n", rf.serverState)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.becomeFollower(false, true)
				rf.persist()
				rf.Info("选举失败，存在更大Term，rf.currentTerm更新为[%d]\n", reply.Term)
				return
			}
			if reply.VoteGranted {
				success++
			}
			if rf.isMajority(success) {
				rf.Info("-----------------------选举成功\n")
				rf.becomeLeader()
			}
		}()
	}
}

func (rf *Raft) appendEntriesRPC() {
	numCommit := 1
	hasCommitted := false

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		i := i
		if i == rf.me {
			continue
		}

		go func() {
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
				bufCopy := make([]LogEntry, lastLogIndex+1-nextIndex)
				copy(bufCopy, rf.logEntries[nextIndex:])
				args.LogEntries = bufCopy
			}
			t := time.Now()
			rf.Info("开始向[%d]发送心跳\n", i)
			rf.mu.Unlock()

			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.Info("向[%d]发送appendEntriesRPC返回，发送时间：%v, ok: %t, 请求:%+v, 回复:%+v\n", i, t, ok, args, reply)

			if !ok || rf.serverState != Leader {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.becomeFollower(false, true)
				rf.persist()
				rf.Info("心跳结束后转变为follower，存在更大Term，rf.currentTerm更新为[%d]\n", reply.Term)
				return
			}
			if len(args.LogEntries) == 0 {
				return
			}

			//发送日志成功
			if reply.Success {
				numCommit++
				if !hasCommitted && numCommit > len(rf.peers)/2 {
					//leader保证提交current term的log时候才能顺带把之前的log提交了
					if rf.currentTerm == args.LogEntries[len(args.LogEntries)-1].Term {
						hasCommitted = true
						rf.commitIndex = prevLogIndex + len(args.LogEntries)
						rf.applyLogs()
					}
				}
				rf.nextIndex[i] = prevLogIndex + len(args.LogEntries) + 1
				rf.matchIndex[i] = prevLogIndex + len(args.LogEntries)
				return
			}
			//如果leader没有conflictTerm的日志，那么重新发送所有日志
			rf.nextIndex[i] = 1
			for j := reply.ConflictIndex; j > 0; j-- {
				if rf.logEntries[j].Term == reply.ConflictTerm {
					rf.nextIndex[i] = j + 1
					break
				}
			}
		}()
	}
}
