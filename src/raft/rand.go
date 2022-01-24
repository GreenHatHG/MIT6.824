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

const min = 6
const max = 12

func (rf *Raft) getRandomInterval() int {
	mu.Lock()
	defer mu.Unlock()
	interval := r.Intn(max-min) + min
	return interval
}