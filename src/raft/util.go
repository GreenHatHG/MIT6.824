package raft

import (
	"../labrpc"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func getTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	min := 450
	max := 600
	timeout := rand.Intn(max-min) + min
	return time.Duration(timeout) * time.Millisecond
}

func sendAppendEntriesToOthers(peers []*labrpc.ClientEnd, me int, args *AppendEntriesArgs, reply *AppendEntriesReply) (uint32, int) {
	var success uint32
	var maxTerm int
	wg := sync.WaitGroup{}
	for i := range peers {
		if i == me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok := peers[i].Call("Raft.AppendEntries", args, reply)
			if ok {
				atomic.AddUint32(&success, 1)
			}
			maxTerm = int(math.Max(float64(maxTerm), float64(reply.Term)))
		}(i)
	}
	wg.Wait()
	return success, maxTerm
}

func sendRequestVoteRPCToOthers(peers []*labrpc.ClientEnd, me int, args *RequestVoteArgs, reply *RequestVoteReply) (uint32, int) {
	var success uint32
	var maxTerm int
	wg := sync.WaitGroup{}
	for i := range peers {
		if i == me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok := peers[i].Call("Raft.RequestVote", args, reply)
			if ok {
				log.Println(me, "start", i, "election", "ok:", ok, "VoteGranted:", reply.VoteGranted)
				atomic.AddUint32(&success, 1)
			}
			maxTerm = int(math.Max(float64(maxTerm), float64(reply.Term)))
		}(i)
	}
	wg.Wait()
	return success, maxTerm
}
