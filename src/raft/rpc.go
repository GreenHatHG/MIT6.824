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
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLogIndex(),
			LastLogTerm:  rf.logEntries[rf.getLastLogIndex()].Term,
		}
		go func() {
			reply := &RequestVoteReply{}
			t := time.Now()

			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.Info("向[%d]索要投票的结果，发送时间: %v, ok: %t, 请求:%+v, 回复:%+v\n", server, t, ok, args, reply)
			if !ok {
				return
			}
			if rf.serverState != Candidate {
				rf.Info("不是Candidate，退出选举，当前状态为[%s]\n", StateString(rf.serverState))
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
	lastLogIndex := rf.getLastLogIndex()

	for i := range rf.peers {
		i := i
		if i == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[i]
		prevLogIndex := nextIndex - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			PrevLogIndex: prevLogIndex,
			LeaderCommit: rf.commitIndex,
			PrevLogTerm:  rf.logEntries[prevLogIndex].Term,
		}
		//要发送给follower的log
		if lastLogIndex >= nextIndex {
			bufCopy := make([]LogEntry, lastLogIndex+1-nextIndex)
			copy(bufCopy, rf.logEntries[nextIndex:])
			args.LogEntries = bufCopy
		}

		go func() {
			reply := &AppendEntriesReply{}
			t := time.Now()

			ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.Info("[%d] appendEntriesRPC返回，发送时间：%v, ok: %t, 请求:%+v, 回复:%+v\n", i, t, ok, args, reply)

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

			//发送日志成功
			if reply.Success && len(args.LogEntries) != 0 {
				rf.nextIndex[i] = prevLogIndex + len(args.LogEntries) + 1
				rf.matchIndex[i] = prevLogIndex + len(args.LogEntries)
				rf.checkMatchIndexMajority()
				return
			}
			if !reply.Success {
				//如果leader没有conflictTerm的日志，那么重新发送所有日志
				rf.nextIndex[i] = 1
				for j := reply.ConflictIndex; j > 0; j-- {
					if rf.logEntries[j].Term == reply.ConflictTerm {
						rf.nextIndex[i] = j + 1
						break
					}
				}
			}
		}()
	}
}
