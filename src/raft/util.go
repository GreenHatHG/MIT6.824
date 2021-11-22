package raft

import (
	"fmt"
	"log"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (rf *Raft) getElectionTimeout() time.Duration {
	min := 450
	max := 600
	timeout := rf.timeoutRand.Intn(max-min) + min
	return time.Duration(timeout) * time.Millisecond
}

func (rf *Raft) isMajority(success uint32) bool {
	majority := success >= uint32((len(rf.peers)/2)+1)
	log.Printf("【%d】接受到的success为[%d]，majority结果为[%t]\n", rf.me, success, majority)
	return majority
}

//todo: 如何正确停止
func (rf *Raft) stopTimer() {
	stopSuccess := rf.electionTimer.Stop()
	log.Printf("【%d】停止选举定时器返回值：%t", rf.me, stopSuccess)
	if !stopSuccess {
		select {
		case <-rf.electionTimer.C: //try to drain from the channel
		default:
		}
	}
}

func (rf *Raft) resetTimeout() {
	rf.stopTimer()
	timeout := rf.getElectionTimeout()
	rf.electionTimer.Reset(timeout)
	log.Printf("【%d】初始化选举定时器超时为%d", rf.me, timeout)
}

func (rf *Raft) getGoroutineId() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func (rf *Raft) sendAppendEntriesToOthers(currentTerm int) (uint32, int) {
	var success uint32
	var maxTerm int
	wg := sync.WaitGroup{}
	maxTermLocker := sync.Mutex{}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			args := AppendEntriesArgs{
				Term: currentTerm,
			}
			reply := AppendEntriesReply{}

			log.Printf("【%d】开始向[%d]发送心跳\n", rf.me, i)
			ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			log.Printf("【%d】发送给[%d]的心跳:%+v, 回复:%+v\n", rf.me, i, args, reply)

			if !ok {
				return
			}

			maxTermLocker.Lock()
			maxTerm = int(math.Max(float64(maxTerm), float64(reply.Term)))
			maxTermLocker.Unlock()
		}(i)
	}
	wg.Wait()
	return success, maxTerm
}

func (rf *Raft) sendRequestVoteRPCToOthers(currentTerm int) (uint32, int) {
	//本身自投一票
	var success uint32 = 1
	var maxTerm int
	wg := sync.WaitGroup{}
	args := &RequestVoteArgs{
		Term:        currentTerm,
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

			log.Printf("【%d】开始向[%d]索要投票\n", rf.me, i)
			ok := rf.peers[i].Call("Raft.RequestVote", args, reply)
			log.Printf("【%d】向[%d]索要投票的结果，ok: %t, 请求:%+v, 回复:%+v\n", rf.me, i, ok, args, reply)

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
