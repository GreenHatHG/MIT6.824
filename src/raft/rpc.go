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
			if !ok || rf.serverState != Candidate || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.serverState = Follower
				rf.resetTerm(reply.Term)
				return
			}
			if reply.VoteGranted {
				success++
			}
			if rf.isMajority(success) {
				rf.Info("-----------------------选举成功\n")
				rf.serverState = Leader
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.logEntries)
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = rf.getLastLogIndex()
				rf.appendEntriesRPC(true)
			}
		}()
	}
}

func (rf *Raft) appendEntriesRPC(empty bool) {
	lastLogIndex := rf.getLastLogIndex()

	for peer := range rf.peers {
		peer := peer
		if peer == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[peer]
		prevLogIndex := nextIndex - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			PrevLogIndex: prevLogIndex,
			LeaderCommit: rf.commitIndex,
			PrevLogTerm:  rf.logEntries[prevLogIndex].Term,
		}
		//要发送给follower的log，如果是empty则不需要带上log，加快rpc速度
		if !empty && lastLogIndex >= nextIndex {
			bufCopy := make([]LogEntry, lastLogIndex+1-nextIndex)
			copy(bufCopy, rf.logEntries[nextIndex:])
			args.LogEntries = bufCopy
		}

		go func() {
			reply := &AppendEntriesReply{}
			t := time.Now()

			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.Info("[%d] appendEntriesRPC返回，发送时间：%v, ok: %t, 请求:%+v, 回复:%+v\n", peer, t, ok, args, reply)
			if !ok || rf.serverState != Leader || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.serverState = Follower
				rf.resetTerm(reply.Term)
				return
			}

			//发送日志成功
			if reply.Success && len(args.LogEntries) != 0 {
				rf.nextIndex[peer] = prevLogIndex + len(args.LogEntries) + 1
				rf.matchIndex[peer] = prevLogIndex + len(args.LogEntries)
				rf.leaderMaybeCommit()
				return
			}
			if !reply.Success {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[peer] = reply.ConflictIndex
					return
				}

				newNextIndex := -1
				for i, entry := range rf.logEntries {
					if entry.Term == reply.ConflictTerm {
						newNextIndex = i
					}
				}
				//找到最后一个term为conflictTerm的log index
				if newNextIndex != -1 {
					rf.nextIndex[peer] = newNextIndex
				} else {
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
		}()
	}
}
