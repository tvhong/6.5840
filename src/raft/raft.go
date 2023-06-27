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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// Tester limits 10 heartbeat per second, and expects new leader to be elected
// within 5s of failure
const (
	electionTimeoutMinMs = 1300
	electionTimeoutMaxMs = 1700
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// persistent
	me          int                 // this peer's index into peers[]
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	currentTerm int                 // The latest term the server has seen
	votedFor    int                 // The peer that this node voted for, -1 means not voted for any node

	// volatile
	mu                  sync.Mutex // Lock to protect shared access to this peer's state
	persister           *Persister // Object to hold this peer's persisted state
	dead                int32      // set by Kill()
	role                Role       // The role of this node
	nextElectionTimeout time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

/************************************************************************
 * RPC handlers
 ***********************************************************************/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf.me, dInfo, "Handle RequestVote from S%v, args: %v", args.CandidateId, args)

	reply.VoteGranted = args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.refreshElectionTimeout()
	}

	reply.Term = rf.currentTerm

	rf.updateTermIfNeeded(args.Term)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf.me, dInfo, "Handle AppendEntries from leader S%v, args: %v", args.LeaderId, args)

	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
	}

	reply.Term = rf.currentTerm

	rf.updateTermIfNeeded(args.Term)
	rf.refreshElectionTimeout()
}

/************************************************************************
 * RPC senders
 ***********************************************************************/
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
	// TODO: deadlock, S0 waits for S2 to complete, S0 cannot send message to S1
	// S0 waits for S1 to respond, S1 waits for S0 to respond
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf.me, dVote, "Request vote from server S%v", server)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.updateTermIfNeeded(reply.Term)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf.me, dInfo, "Send appendEntries to server S%v", server)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.updateTermIfNeeded(reply.Term)

	return ok
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		Debug(rf.me, dTimer, "Tick")

		if rf.role == Leader {
			rf.sendHeartbeats()
		} else {
			if time.Now().After(rf.nextElectionTimeout) {
				Debug(rf.me, dVote, "Election timeout!")
				rf.role = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.refreshElectionTimeout()
				Debug(rf.me, dVote, "Convert to Candidate with term %v", rf.currentTerm)

				for peer := 0; peer < len(rf.peers); peer++ {
					if peer == rf.me {
						continue
					}

					args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
					reply := RequestVoteReply{}
					go rf.sendRequestVote(peer, &args, &reply)

					// TODO: handle reply. Track if got majority votes
					// Create voteReceived
					// If timeout, clear the list that waits for reponse
					// If convert to Follower, clear the list that waits for response
				}
			}
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

/************************************************************************
 * Helper methods
 ***********************************************************************/
func (rf *Raft) refreshElectionTimeout() {
	rf.nextElectionTimeout = time.Now().
		Add(time.Duration(Random(electionTimeoutMinMs, electionTimeoutMaxMs) * int(time.Millisecond)))
}

func (rf *Raft) sendHeartbeats() {
	Debug(rf.me, dTimer, "Leader, sending heartbeats")
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(peer, &args, &reply)
	}
}

func (rf *Raft) updateTermIfNeeded(term int) {
	if term > rf.currentTerm {
		Debug(rf.me, dState, "Received newer term %v > %v. Converting to Follower.", term, rf.currentTerm)
		rf.currentTerm = term
		rf.role = Follower
	}
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
func Make(peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// TODO (2C): read from persisted states
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	//TODO: shouldn't hardcode leader
	if me == 2 {
		rf.role = Leader
	}

	rf.refreshElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(me, dLog, "Hello World!")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
