package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type ServerState int

const (
	Leader ServerState = iota
	Follower
	Candidate
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	//2A
	serverState ServerState
	//initialized to 0 on first boot, increases monotonically
	//latest term server has seen
	currentTerm int
	//candidateId that received vote in current term (or null if none)
	votedFor         int
	timer            *time.Timer
	heartbeatsTicker *time.Ticker

	//2B
	logEntries  []interface{}
	commitIndex int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.serverState == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//candidate’s term
	Term int
	//candidate requesting vote
	CandidateId int
	//index of candidate’s last log entry
	LastLogIndex int
	//term of candidate’s last log entry
	LastLogTerm string
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//currentTerm, for candidate to update itself
	Term int
	//true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term >= rf.currentTerm {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimeout()
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.serverState = Follower
	}
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	//leader’s term
	Term int

	//2B
	Entry interface{}
	//leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	//currentTerm, for leader to update itself
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if 
	if args.Term > rf.currentTerm {
		rf.serverState = Follower
		rf.resetTimeout()
	} else {
		reply.Term = rf.currentTerm
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// 使用Raft附加一条新command，如果此server不是leader，返回false，否则立即开始并立刻返回
// 没有保证这个命令会被提交到Raft日志，因为领导人可能会失败或输掉选举
// 即使Raft实例已经被终止，这个函数也应该优雅地返回
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	isLeader := rf.serverState == Leader
	if isLeader {
		go func() {
			rf.mu.Lock()
			rf.logEntries = append(rf.logEntries, command)
			rf.mu.Unlock()
			sendAppendEntriesToOthers(rf.peers, rf.me, &AppendEntriesArgs{
				Term:         rf.currentTerm,
				Entry:        command,
				LeaderCommit: rf.commitIndex,
			}, &AppendEntriesReply{})
		}()
	}
	return rf.commitIndex + 1, rf.currentTerm, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	if rf.heartbeatsTicker != nil {
		rf.heartbeatsTicker.Stop()
	}
	// Your code here, if desired.
	log.Println(rf.me, "kill")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func (rf *Raft) election() bool {
	rf.serverState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetTimeout()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	reply := &RequestVoteReply{}

	success, maxTerm := sendRequestVoteRPCToOthers(rf.peers, rf.me, args, reply)
	if maxTerm > rf.currentTerm {
		rf.currentTerm = maxTerm
		rf.serverState = Follower
		return false
	}
	if success+1 > uint32(len(rf.peers))/2 {
		log.Println("isElectionSuccess", rf.me)
		rf.serverState = Leader
		rf.timer.Stop()
		go rf.startHeartbeats()
		return true
	}
	return false
}

func (rf *Raft) startHeartbeats() {
	rf.heartbeatsTicker = time.NewTicker(150 * time.Millisecond)
	go func() {
		for {
			select {
			case <-rf.heartbeatsTicker.C:
				log.Println(rf.me, "heartbeats")
				_, maxTerm := sendAppendEntriesToOthers(rf.peers, rf.me, &AppendEntriesArgs{}, &AppendEntriesReply{})
				if maxTerm > rf.currentTerm {
					rf.heartbeatsTicker.Stop()
					rf.serverState = Follower
					return
				}
			}
		}
	}()
}

func (rf *Raft) resetTimeout() {
	timeout := getTimeout()
	log.Println(rf.me, "getTimeOut", timeout)
	rf.timer.Reset(timeout)
}

func (rf *Raft) electionTimer() {
	rf.timer = time.NewTimer(getTimeout())
	go func() {
		for {
			select {
			case <-rf.timer.C:
				log.Println(rf.me, "timeout")
				rf.election()
			}
		}
	}()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.serverState = Follower
	rf.votedFor = -1
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	go rf.electionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
