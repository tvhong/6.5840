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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type void struct{}

var member void

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// Tester limits 10 heartbeat per second, and expects new leader to be elected
// within 5s of failure
const (
	initElectionTimeoutMinMs = 100
	initElectionTimeoutMaxMs = 300

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
	log         []LogEntry

	// volatile
	mu                  sync.Mutex // Lock to protect shared access to this peer's state
	persister           *Persister // Object to hold this peer's persisted state
	dead                int32      // set by Kill()
	role                Role       // The role of this node
	nextElectionTimeout time.Time
	votesReceived       map[int]void // Set containing the votes received, elements are server ids

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
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

	Debug(rf.me, rf.currentTerm, dVote, "Receive RequestVote from S%v, args: %v", args.CandidateId, args)

	rf.maybeAdvanceTerm(args.Term)
	reply.Term = rf.currentTerm

	reply.VoteGranted = args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		Debug(rf.me, rf.currentTerm, dVote, "Vote for S%v", args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.refreshElectionTimeout()
	} else {
		Debug(rf.me, rf.currentTerm, dVote, "Do not vote for server S%v", args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf.me, rf.currentTerm, dRpc, "Receive AppendEntries from S%v, args: %v", args.LeaderId, args)

	rf.maybeAdvanceTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		Debug(rf.me, rf.currentTerm, dRpc,
			"Reject AppendEntries from S%v. args.Term (%v) < rf.currentTerm (%v)", args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
	} else if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Debug(rf.me, rf.currentTerm, dRpc,
			"Reject AppendEntries from S%v. len(rf.log) (%v) <= PrevLogIndex (%v) || rf.log[PrevLogIndex].Term (%v) != PrevLogTerm (%v)",
			args.LeaderId,
			len(rf.log),
			args.PrevLogIndex,
			rf.log[args.PrevLogIndex].Term,
			args.PrevLogTerm)

		reply.Success = false
	} else {
		conflictEntryIndex := -1
		conflictLogIndex := -1
		n := Min(len(args.Entries), len(rf.log)-args.PrevLogIndex)
		for i := 0; i < n; i++ {
			j := args.PrevLogIndex + 1 + i
			if args.Entries[i] != rf.log[j] {
				conflictEntryIndex = i
				conflictLogIndex = j

				Debug(rf.me, rf.currentTerm, dRpc,
					"Found conflicting log at index %v. Entry in log: %v, entry in args: %v. Leader Id: %v",
					conflictLogIndex,
					rf.log[conflictLogIndex],
					args.Entries[conflictEntryIndex],
					args.LeaderId)

				break
			}
		}

		// 3. 
		if conflictLogIndex != -1 {
			Debug(rf.me, rf.currentTerm, dRpc, "Delete conflicting logs from rf.log index %v onward.", conflictLogIndex)

			rf.log = rf.log[:conflictLogIndex]
		}

		// 4. Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries[len(rf.log)-args.PrevLogIndex:]...)

		// hasNewEntries = hasConflict || prevIndex + len(entries) > len(rf.log)
		// startCopyEntryIndex = hasConflict ? conflictEntryIndex : len(rf.log) - prevLogIndex
		// rf.log = append(rf.log, args.Entry[startCopyEntryIndex:]...)

		// if noConflict and prevIndex + len(entries) <= len(rf.log), no-op
		// startCopyEntryIndex = hasConflict ? conflictIndex : len(rf.log)
		// if noConflict: loop thorugh all entries and assign to corresponding rf.log
		// if conflictEntryIndex > len(args.Entries), no-op
		// if conflictLogIndex == len(rf.log), rf.log = append(rf.log, args.Entry[conflictEntryIndex:]...)
		// if conflictLogIndex > len(rf.log), throw exception
		// Copy data from entries[conflictEntryIndex] into

		// for i := Max(args.PrevLogIndex+1, conflictIndex); i < args.PrevLogIndex + n; i++ {
		// 	rf.log = append(rf.log, args.Entries[i])
		// }

		Debug(rf.me, rf.currentTerm, dRpc, "Accept AppendEntries from S%v", args.LeaderId)
		reply.Success = true
	}

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
	rf.mu.Lock()

	Debug(rf.me, rf.currentTerm, dVote, "Request vote from server S%v", server)

	currTerm := rf.currentTerm

	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()

	if rf.currentTerm == currTerm {
		advancedTerm := rf.maybeAdvanceTerm(reply.Term)

		if reply.VoteGranted {
			if advancedTerm {
				log.Fatalf("S%v shouldn't have voted for S%v if it has higher term, currentTerm: %v, reply: %v", server, rf.me, rf.currentTerm, reply)
			}

			rf.votesReceived[server] = member

			// Received majority votes
			if len(rf.votesReceived) >= len(rf.peers)/2+1 {
				Debug(rf.me, rf.currentTerm, dVote, "Receive majority votes, becoming leader. votesReceived: %v", rf.votesReceived)
				rf.becomeLeader()
			}
		}
	} else {
		Debug(rf.me, rf.currentTerm, dVote, "Received RequestVote response from S%v for term %v, but this node's term has changed", server, currTerm)
	}

	rf.mu.Unlock()

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	rf.mu.Lock()
	Debug(rf.me, rf.currentTerm, dRpc, "Send appendEntries to server S%v", server)
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	rf.maybeAdvanceTerm(reply.Term)

	// TODO: create new data structure to track responses
	// TODO: count my own vote as yes

	// If majority responded, commit
	//   applyCh when majority vote received (increase lastApplied)
	//   Increase commitIndex
	//   Clean up tracking map once majority vote received
	// Handle unknown logIndex (not found in tracking map or <commitIndex)
	rf.mu.Unlock()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(rf.me, rf.currentTerm, dAppend, "Start a command len(rf.log)=%v", len(rf.log))

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		nextIndex := rf.nextIndex[peer]
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.log[nextIndex].Term,
			Entries:      rf.log[rf.nextIndex[peer]:len(rf.log)], // TODO: assert nextIndex <= len(rf.log)
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(peer, &args, &reply)
	}

	return len(rf.log), rf.currentTerm, true
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

		Debug(rf.me, rf.currentTerm, dTimer, "Tick")

		if rf.role == Leader {
			rf.sendHeartbeats()
		} else {
			if time.Now().After(rf.nextElectionTimeout) {
				Debug(rf.me, rf.currentTerm, dVote, "Election timeout!")
				rf.role = Candidate
				rf.advanceTerm(rf.currentTerm + 1)

				rf.votedFor = rf.me
				rf.votesReceived = make(map[int]void)
				rf.votesReceived[rf.me] = member

				rf.refreshElectionTimeout()

				Debug(rf.me, rf.currentTerm, dVote, "Convert to Candidate with term %v", rf.currentTerm)

				for peer := 0; peer < len(rf.peers); peer++ {
					if peer == rf.me {
						continue
					}

					args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
					reply := RequestVoteReply{}
					go rf.sendRequestVote(peer, &args, &reply)

					// TODO: handle reply. Track if got majority votes
					// Create votesReceived
					// If timeout, clear votesReceived
					// If convert to Follower, clear votesReceived
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
func (rf *Raft) initElectionTimeout() {
	rf.nextElectionTimeout = time.Now().
		Add(time.Duration(Random(initElectionTimeoutMinMs, initElectionTimeoutMaxMs) * int(time.Millisecond)))
}

func (rf *Raft) refreshElectionTimeout() {
	rf.nextElectionTimeout = time.Now().
		Add(time.Duration(Random(electionTimeoutMinMs, electionTimeoutMaxMs) * int(time.Millisecond)))
}

func (rf *Raft) sendHeartbeats() {
	Debug(rf.me, rf.currentTerm, dTimer, "Leader, sending heartbeats")
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(peer, &args, &reply)
	}
}

func (rf *Raft) maybeAdvanceTerm(term int) bool {
	advance := term > rf.currentTerm
	if advance {
		Debug(rf.me, rf.currentTerm, dState, "Receive newer term %v > %v. Converting to Follower.", term, rf.currentTerm)
		rf.advanceTerm(term)
		prevRole := rf.role
		rf.role = Follower

		if prevRole == Leader {
			rf.refreshElectionTimeout()
		}
	}

	return advance
}

func (rf *Raft) advanceTerm(term int) {
	if term <= rf.currentTerm {
		log.Fatalf("Updating currentTerm %v with a smaller or equal value: %v", rf.currentTerm, term)
	}

	rf.currentTerm = term
	rf.votedFor = -1
	rf.votesReceived = nil
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	rf.sendHeartbeats()
}

/************************************************************************
 * Factory
 ***********************************************************************/
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]interface{}, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	rf.initElectionTimeout()

	// TODO (2C): read from persisted states
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(rf.me, rf.currentTerm, dLog, "Hello World!")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
