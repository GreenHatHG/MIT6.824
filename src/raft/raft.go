package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new raftLog entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the raftLog, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"

	"sync"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

// ApplyMsg
// as each Raft peer becomes aware that successive raftLog entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed raftLog entry.
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

type ServerState int

const (
	Leader ServerState = iota
	Follower
	Candidate
)

func StateString(state ServerState) string {
	switch state {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return ""
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A
	timeoutInterval int
	tick            func()
	serverState     ServerState
	votedFor        int
	currentTerm     int
	//2B
	logEntries []LogEntry
	//index of the highest log entry known to be  committed (initialized to 0, increases monotonically)
	commitIndex int
	//对于每个server，发送到该server的下一个log entry的index(initialized to leader last log index + 1)
	nextIndex []int
	//对于每个server，已知要在server上复制的最新log entry的index
	matchIndex  []int
	applyMsg    chan ApplyMsg
	lastApplied int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.serverState == Leader
	return term, isLeader
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

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//candidate’s term
	Term int
	//candidate requesting vote
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
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

type AppendEntriesArgs struct {
	//leader’s term
	Term int
	//2B
	//index of log entry immediately preceding new ones
	PrevLogIndex int
	//term of prevLogIndex entry
	PrevLogTerm int
	//log entries to store (empty for heartbeat;
	//may send more than one for efficiency)
	LogEntries []LogEntry
	//leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	//currentTerm, for leader to update itself
	Term int
	//2B
	Success bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Log("处理RequestVote: %+v\n", args)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	if voteOrNot := rf.votedFor == -1 || rf.votedFor == args.CandidateId; !voteOrNot {
		return
	}
	//确保日志至少和接收者一样新
	if args.LastLogTerm < rf.logEntries[len(rf.logEntries)-1].Term {
		return
	}
	if args.LastLogTerm == rf.logEntries[len(rf.logEntries)-1].Term && args.LastLogIndex < len(rf.logEntries)-1 {
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	rf.currentTerm = args.Term
	rf.Log("处理RequestVote中存在更大Term，更新currentTerm为[%d]\n", args.Term)
	rf.serverState = Follower
	rf.timeoutInterval = 0
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Log("处理AppendEntries: %+v\n", args)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	rf.currentTerm = args.Term
	rf.Log("更新currentTerm为[%d]\n", rf.currentTerm)
	rf.becomeFollower()

	//日志缺少
	if args.PrevLogIndex > len(rf.logEntries) {
		rf.Log("PrevLogIndex位置缺少日志\n")
		return
	}
	//delete the conflicted entry and all that follow it
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.Log("存在冲突log\n")
		rf.logEntries = rf.logEntries[:args.PrevLogIndex]
		return
	}

	reply.Success = true
	for _, entry := range args.LogEntries {
		rf.logEntries = append(rf.logEntries, entry)
		rf.Log("apply msg: %+v\n", entry)
		rf.applyMsg <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.commitIndex + 1,
		}
		rf.commitIndex++
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.logEntries)-1)
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

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's raftLog. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft raftLog, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.serverState == Leader
	index := rf.commitIndex + 1

	if isLeader {
		rf.Log("-------------------接收到command: %+v", command)
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.nextIndex[rf.me]++
	}

	return index, rf.currentTerm, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here, if desired.
	log.Println("--------------------------------------", rf.me, "kill")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Your initialization code here (2A, 2B, 2C).
	rf.applyMsg = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logEntries = make([]LogEntry, 0)
	//index从1开始
	rf.logEntries = append(rf.logEntries, LogEntry{})

	rf.becomeFollower()
	go rf.ticker()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}