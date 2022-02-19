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
				rf.becomeFollower(false, true, reply.Term)
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
	for peer := range rf.peers {
		peer := peer
		if peer == rf.me {
			continue
		}

		lastLogIndex := rf.getLastLogIndex()
		nextIndex := rf.nextIndex[peer]
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

			ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.Info("[%d] appendEntriesRPC返回，发送时间：%v, ok: %t, 请求:%+v, 回复:%+v\n", peer, t, ok, args, reply)
			if !ok || rf.serverState != Leader || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(false, true, reply.Term)
				rf.Info("心跳结束后转变为follower，存在更大Term，rf.currentTerm更新为[%d]\n", reply.Term)
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
