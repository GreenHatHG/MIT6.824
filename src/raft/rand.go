package raft

import (
	"math/rand"
	"sync"
	"time"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex
)

const min = 250
const max = 400
const heartBeatInterval = 100 * time.Millisecond

func (rf *Raft) getRandomElectionTimeout() time.Duration {
	mu.Lock()
	defer mu.Unlock()
	interval := r.Intn(max-min) + min
	rf.Info("获取到ElectionTimeout: %d", interval)
	return time.Duration(interval) * time.Millisecond
}
