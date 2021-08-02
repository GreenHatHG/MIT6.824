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
	"math/rand"
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
//当每个 Raft peer 意识到连续的日志条目被提交时，peer 应该通过传递给 Make() 的 applyCh
//向同一服务器上的服务（或测试者）发送一个 ApplyMsg。 将 CommandValid 设置为 true
//以指示 ApplyMsg 包含新提交的日志条目。
//在实验 3 中，您需要在 applyCh 上发送其他类型的消息（例如，快照）；
//此时您可以向 ApplyMsg 添加字段，但将 CommandValid 设置为 false 以用于其他用途。
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
	serverState ServerState
	//initialized to 0 on first boot, increases monotonically
	//latest term server has seen
	currentTerm int
	//candidateId that received vote in current term (or null if none)
	votedFor              int
	electionLastAccessed  time.Time
	isHeartbeatTickerLive uint32
	newRand               *rand.Rand

	//2B
	logEntries []LogEntry
	//index of highest log entry known to be  committed (initialized to 0, increases monotonically)
	commitIndex int
	//对于每个server，发送到该server的下一个log entry的index
	nextIndex []int
	//对于每个server，已知要在server上复制的最新log entry的index
	matchIndex []int
	applyMsg   chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
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
	log.Printf("%d 处理RequestVote: %+v", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}
	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		return
	}
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		return
	}

	//已经投过票，但是要即将要票的term大于当前term
	//还没有投过票
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.electionLastAccessed = time.Now()

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	log.Println(rf.me, "更新currentTerm为", rf.currentTerm)
	rf.serverState = Follower
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
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Xterm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.logEntries)

	//AppendEntries RPC Receiver implementation 1
	if rf.currentTerm > args.Term {
		return
	}

	rf.serverState = Follower
	rf.electionLastAccessed = time.Now()
	log.Println(rf.me, "收到心跳，重置timeout", rf.electionLastAccessed)

	//第一个log或者log一致
	if args.PrevLogIndex == -1 || args.Term == rf.logEntries[args.PrevLogIndex].Term {
		rf.logEntries = append(rf.logEntries, args.LogEntries...)
		reply.Success = true
		//AppendEntries RPC Receiver implementation 5
		if args.LeaderCommit > rf.commitIndex {
			min := minInt(args.LeaderCommit, len(rf.logEntries)-1)
			for i := rf.commitIndex + 1; i <= min; i++ {
				log.Printf("%d apply msg: %+v\n", rf.me, rf.logEntries[i])
				rf.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[i].Command,
					CommandIndex: rf.logEntries[i].Index + 1,
				}
			}
		}
		return
	}

	reply.Xterm = rf.logEntries[args.PrevLogIndex].Term
	for i, v := range rf.logEntries {
		if v.Term == reply.Xterm {
			reply.XIndex = i
			break
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.serverState == Leader
	log.Println(rf.me, "isLeader:", isLeader, " 接收到command", command)

	// Your code here (2B).
	if !isLeader {
		return -1, -1, false
	}

	rf.logEntries = append(rf.logEntries, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   len(rf.logEntries),
	})
	rf.nextIndex[rf.me]++

	return len(rf.logEntries), rf.currentTerm, isLeader
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
	if atomic.LoadUint32(&rf.isHeartbeatTickerLive) == 1 {
		atomic.StoreUint32(&rf.isHeartbeatTickerLive, 0)
	}
	// Your code here, if desired.
	rf.mu.Lock()
	log.Println(rf.me, rf.serverState, "已经被kill")
	rf.mu.Unlock()
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

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.serverState = Candidate
	rf.currentTerm++
	log.Println(rf.me, "更新currentTerm为", rf.currentTerm)
	rf.votedFor = rf.me
	log.Println(rf.me, " 选举开始 重置心跳")
	rf.electionLastAccessed = time.Now()
	rf.mu.Unlock()

	success, maxTerm := rf.sendRequestVoteRPCToOthers()
	log.Println(rf.me, "选举结果", success, maxTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if maxTerm > rf.currentTerm {
		rf.resetToFollower()
		rf.currentTerm = maxTerm
		log.Println("选举成功，出现更大term", rf.me, "更新currentTerm为", rf.currentTerm)
		return
	}

	if rf.isMajority(success) {
		log.Println("选举成功", rf.me)
		rf.serverState = Leader
		rf.startHeartbeats()
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.logEntries)
		}
		return
	}

	rf.resetToFollower()
	return
}

func (rf *Raft) startHeartbeats() {
	//已经开启
	if atomic.LoadUint32(&rf.isHeartbeatTickerLive) == 1 {
		return
	}
	//否则则启动一个定时器发送心跳
	atomic.StoreUint32(&rf.isHeartbeatTickerLive, 1)
	go func() {
		for atomic.LoadUint32(&rf.isHeartbeatTickerLive) == 1 {
			log.Println(rf.me, "定时器生效后发送心跳")
			rf.sendAppendEntriesToOthers()
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (rf *Raft) manageLifecycle() {
	for {
		rf.mu.Lock()
		state := rf.serverState
		rf.mu.Unlock()
		if state == Follower {
			rf.electionTimer()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) electionTimer() {
	timeout := rf.getTimeout()
	log.Println(rf.me, "选举定时器睡眠", timeout)
	time.Sleep(timeout)

	rf.mu.Lock()
	lastAccessed := rf.electionLastAccessed
	rf.mu.Unlock()

	if time.Now().Sub(lastAccessed).Milliseconds() >= timeout.Milliseconds() {
		log.Println(rf.me, "timeout到钟，发起选举", lastAccessed)
		rf.election()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.serverState = Follower
	rf.applyMsg = applyCh
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logEntries = make([]LogEntry, 0)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	rf.newRand = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me*10000)))
	go rf.manageLifecycle()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
