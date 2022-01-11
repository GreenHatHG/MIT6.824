package raft

import (
	"math"
	"time"
)

type requestVoteRes struct {
	majority bool
	maxTerm  int
}

func (rf *Raft) requestVoteRPC(currentTerm int) (bool, int) {
	replyC := make(chan *RequestVoteReply, len(rf.peers)-1)
	resC := make(chan requestVoteRes)
	go rf.requestVoteReplyHandler(replyC, resC)

	args := &RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}

	for server := range rf.peers {
		server := server
		if server == rf.me {
			continue
		}
		go func() {
			reply := &RequestVoteReply{}

			t := time.Now()
			rf.raftLog.Printf("开始向[%d]索要投票\n", server)
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			rf.raftLog.Printf("向[%d]索要投票的结果，发送时间: %v, ok: %t, 请求:%+v, 回复:%+v\n", server, t, ok, args, reply)

			if !ok {
				return
			}
			replyC <- reply
		}()
	}
	res := <-resC
	return res.majority, res.maxTerm
}

func (rf *Raft) requestVoteReplyHandler(replyC <-chan *RequestVoteReply, res chan<- requestVoteRes) {
	//本身自投一票
	success, maxTerm := 1, 0
	for reply := range replyC {
		if reply.VoteGranted {
			success++
		}
		maxTerm = int(math.Max(float64(maxTerm), float64(reply.Term)))
		if rf.isMajority(success) {
			res <- requestVoteRes{true, maxTerm}
			return
		}
	}
	res <- requestVoteRes{}
}

func (rf *Raft) appendEntriesRPC(currentTerm int) int {
	replyC := make(chan *AppendEntriesReply, len(rf.peers)-1)
	resC := make(chan int, 1)
	go rf.appendEntriesReplyHandler(replyC, resC, currentTerm)

	for server := range rf.peers {
		server := server
		if server == rf.me {
			continue
		}
		go func() {
			args := &AppendEntriesArgs{
				Term: currentTerm,
			}
			reply := &AppendEntriesReply{}

			t := time.Now()
			rf.raftLog.Printf("开始向[%d]发送心跳\n", server)
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			rf.raftLog.Printf("向[%d]发送心跳，发送时间：%v, ok: %t, 请求:%+v, 回复:%+v\n", server, t, ok, args, reply)

			if !ok {
				return
			}
			replyC <- reply
		}()
	}
	return <-resC
}

func (rf *Raft) appendEntriesReplyHandler(replyC <-chan *AppendEntriesReply, res chan<- int,
	currentTerm int) {
	for reply := range replyC {
		if reply.Term > currentTerm {
			res <- reply.Term
			return
		}
	}
	res <- 0
}
