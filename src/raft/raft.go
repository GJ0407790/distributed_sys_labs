package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

const HeartbeatInterval = 100
const MaxElectionInterval = 1200
const MinElectionInterval = 1000

const (
	Follower  = iota
	Leader    = iota
	Candidate = iota
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct{}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currTerm      int
	votedFor      int
	state         int
	logs          []Log
	commitIdx     int
	lastApplied   int
	peersNextIdx  []int
	peersMatchIdx []int

	// For Follower
	lastReceived time.Time

	// For Leader
	lastSentAppend time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currTerm
	isleader = rf.state == Leader

	return term, isleader
}

// All convert functions assume that the lock has been held
func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.lastReceived = time.Now()
	rf.votedFor = rf.me
	rf.currTerm++
}

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.state = Follower
	rf.currTerm = newTerm
	rf.votedFor = -1
	rf.lastReceived = time.Now()
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader
	rf.lastReceived = time.Now()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandId      int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reqTerm, reqCandId := args.Term, args.CandId

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]: Receive RequestVote message from %d with term %d.", rf.me, reqCandId, reqTerm)

	// The request term is behind the current term
	if rf.currTerm > reqTerm {
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	}

	if rf.currTerm < reqTerm {
		rf.ConvertToFollower(reqTerm)
	}

	reply.Term = rf.currTerm
	reply.VoteGranted = false

	if rf.votedFor < 0 {
		// vote for this candidate
		DPrintf("[%d]: Voted for %d with term %d.", rf.me, reqCandId, reqTerm)
		rf.votedFor = reqCandId
		rf.lastReceived = time.Now()
		reply.VoteGranted = true
	}

	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.ConvertToCandidate()

	args := RequestVoteArgs{
		Term:        rf.currTerm,
		CandId:      rf.me,
		LastLogIdx:  0,
		LastLogTerm: 0,
	}

	numVote := 1
	rf.mu.Unlock()

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			go func(p int) {
				reply := RequestVoteReply{}

				ok := rf.sendRequestVote(p, &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currTerm {
					rf.ConvertToFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					numVote++
				}

				_, isLeader := rf.GetState()
				if !isLeader && (2*numVote >= len(rf.peers)) {
					rf.ConvertToLeader()
					go rf.callHeartbeatFunc()
				}

			}(peer)
		}
	}
}

func (rf *Raft) electionRoutine() {
	for {
		// Election timeout ranges from 150ms to 300ms
		interval := MinElectionInterval + rand.Int31n(MaxElectionInterval-MinElectionInterval)

		startTime := time.Now()
		time.Sleep(time.Duration(interval) * time.Millisecond)
		rf.mu.Lock()

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.lastReceived.Before(startTime) {
			if rf.state != Leader {
				go rf.startElection()
			}
		}
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIdx      int
	PrevLogTerm     int
	Entries         []Log
	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reqTerm, leaderId := args.Term, args.LeaderId

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastReceived = time.Now()

	DPrintf("[%d]: Receive AppendEntries message from %d with term %d.", rf.me, leaderId, reqTerm)

	if rf.currTerm > reqTerm {
		reply.Term = rf.currTerm
		reply.Success = false
		return
	}

	if rf.currTerm < reqTerm {
		rf.ConvertToFollower(reqTerm)
	}

	reply.Term = rf.currTerm
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) callHeartbeat(args *AppendEntriesArgs) {
	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		}

		DPrintf("[%d]: Sent heartbeat message to %d.", rf.me, index)
		go func(peer int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(peer, args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.lastSentAppend = time.Now()
			if reply.Term > rf.currTerm {
				rf.state = Follower
			}
		}(index)
	}
}

func (rf *Raft) callHeartbeatFunc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// The leader has been idling for more than 150ms - 300ms
	heartbeatArgs := AppendEntriesArgs{
		Term:            rf.currTerm,
		LeaderId:        rf.me,
		PrevLogIdx:      0,
		PrevLogTerm:     0,
		Entries:         make([]Log, 0),
		LeaderCommitIdx: 0,
	}

	rf.lastSentAppend = time.Now()
	go rf.callHeartbeat(&heartbeatArgs)
}

func (rf *Raft) heartbeatRoutine() {
	for {
		startTime := time.Now()
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)

		rf.mu.Lock()

		_, isLeader := rf.GetState()

		if !isLeader || rf.killed() {
			rf.mu.Unlock()
			continue
		}

		if rf.lastSentAppend.Before(startTime) {
			go rf.callHeartbeatFunc()
		}

		rf.mu.Unlock()
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.logs = make([]Log, len(peers))
	rf.commitIdx = 0
	rf.lastApplied = 0
	rf.peersNextIdx = make([]int, len(peers))
	rf.peersMatchIdx = make([]int, len(peers))

	rf.lastReceived = time.Now()
	rf.lastSentAppend = time.Now()

	// Start the goroutine here:
	// 1. The heartbeat routine
	// 2. The timeout routine for election
	go rf.heartbeatRoutine()
	go rf.electionRoutine()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
