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
	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"log"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	follower         = 0
	candidate        = 1
	leader           = 2
	heartbeatTimeOut = 100 * time.Millisecond
)

func newElectionTimeOut() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state int

	currentTerm  int
	votedFor     int
	log          []Entry
	lastIncluded int
	snapshot     []byte

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	voteCount    int
	needPersist  bool
	timer        *time.Timer
	commitUpdate *sync.Cond

	applyCh chan ApplyMsg
}

type Entry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	if rf.needPersist {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		if e.Encode(rf.currentTerm) != nil ||
			e.Encode(rf.votedFor) != nil ||
			e.Encode(rf.log) != nil ||
			e.Encode(rf.lastIncluded) != nil {
			log.Fatal("failed to encode raft persistent state")
		}
		raftstate := w.Bytes()
		rf.persister.Save(raftstate, rf.snapshot)
		rf.needPersist = false
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log_ []Entry
	var lastIncluded int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log_) != nil ||
		d.Decode(&lastIncluded) != nil {
		log.Fatal("failed to decode raft persistent state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log_
		rf.lastIncluded = lastIncluded
		rf.commitIndex = lastIncluded
		rf.lastApplied = lastIncluded
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	if index > rf.lastIncluded && index < rf.lastIncluded+len(rf.log) {
		rf.log = rf.log[index-rf.lastIncluded:]
		rf.lastIncluded = index
		rf.snapshot = snapshot
		rf.needPersist = true
	}
	rf.persist()
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	var term int
	var voteGranted bool
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	if rf.state == follower && args.Term == rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logAtLeastUpToDate(args) {
			voteGranted = true
			if rf.votedFor == -1 {
				rf.votedFor = args.CandidateId
				rf.needPersist = true
			}
			rf.timer.Reset(newElectionTimeOut())
		}
	}
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	reply.Term = term
	reply.VoteGranted = voteGranted
}

func (rf *Raft) updateTerm(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.needPersist = true
	rf.timer.Reset(newElectionTimeOut())
}

func (rf *Raft) logAtLeastUpToDate(args *RequestVoteArgs) bool {
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
		return true
	}
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
		args.LastLogIndex >= rf.lastIncluded+len(rf.log)-1 {
		return true
	}
	return false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.state == candidate && reply.Term == rf.currentTerm {
		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount == len(rf.peers)/2 {
				rf.state = leader
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.lastIncluded + len(rf.log)
				}
				for i := range rf.matchIndex {
					rf.matchIndex[i] = 0
				}
				rf.sendHeartbeat()
				rf.timer.Reset(heartbeatTimeOut)
			}
		}
	} else if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
	}
	rf.persist()
	rf.mu.Unlock()
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
	isLeader := false

	// Your code here (2B).

	rf.mu.Lock()
	if rf.state == leader {
		rf.log = append(rf.log, Entry{command, rf.currentTerm})
		rf.needPersist = true
		rf.persist()
		rf.timer.Reset(heartbeatTimeOut)
		rf.sendHeartbeat()
		index = rf.lastIncluded + len(rf.log) - 1
		term = rf.currentTerm
		isLeader = true
	}
	rf.mu.Unlock()
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

	rf.votedFor = -1
	rf.log = []Entry{{nil, 0}}
	rf.nextIndex = make([]int, len(peers), len(peers))
	rf.matchIndex = make([]int, len(peers), len(peers))
	rf.commitUpdate = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.timer = time.NewTimer(newElectionTimeOut())

	// start ticker goroutine to start elections
	go rf.applyChSender()
	go rf.ticker()
	return rf
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.timer.C
		rf.mu.Lock()
		switch rf.state {
		case follower, candidate:
			rf.startElections()
			rf.timer.Reset(newElectionTimeOut())
		case leader:
			rf.sendHeartbeat()
			rf.timer.Reset(heartbeatTimeOut)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElections() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.needPersist = true
	rf.persist()
	rf.voteCount = 0
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastIncluded + len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		go rf.sendRequestVote(i, &args, &RequestVoteReply{})
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.state == leader && reply.Term == rf.currentTerm {
		if reply.Success {
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
				if newMatchIndex > rf.commitIndex {
					rf.updateCommit()
				}
			}
		} else {
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				newNextIndex := rf.lastIncluded + len(rf.log) - 1
				for ; newNextIndex >= rf.lastIncluded; newNextIndex-- {
					if rf.log[newNextIndex-rf.lastIncluded].Term <= reply.Term {
						break
					}
				}
				if newNextIndex == rf.lastIncluded-1 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else if rf.log[newNextIndex-rf.lastIncluded].Term != reply.ConflictTerm {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					rf.nextIndex[server] = newNextIndex
				}
			}
		}
	} else if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) updateCommit() {
	for newCommit := rf.lastIncluded + len(rf.log) - 1; newCommit > rf.commitIndex; newCommit-- {
		if rf.log[newCommit-rf.lastIncluded].Term != rf.currentTerm {
			return
		}
		count := 0
		for _, v := range rf.matchIndex {
			if v >= newCommit {
				count++
				if count == len(rf.peers)/2 {
					rf.commitIndex = newCommit
					rf.commitUpdate.Signal()
					return
				}
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var term int
	var success bool
	var conflictIndex int
	var conflictTerm int
	rf.mu.Lock()
	if rf.state == candidate && args.Term == rf.currentTerm {
		rf.state = follower
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	if rf.state == follower && args.Term == rf.currentTerm {
		if args.PrevLogIndex < rf.lastIncluded {
			conflictTerm = -1
			conflictIndex = rf.lastIncluded + 1
		} else if args.PrevLogIndex > rf.lastIncluded+len(rf.log)-1 {
			conflictTerm = -1
			conflictIndex = rf.lastIncluded + len(rf.log)
		} else if rf.log[args.PrevLogIndex-rf.lastIncluded].Term != args.PrevLogTerm {
			conflictTerm = rf.log[args.PrevLogIndex-rf.lastIncluded].Term
			conflictIndex = args.PrevLogIndex
			for ; conflictIndex >= rf.lastIncluded+1; conflictIndex-- {
				if rf.log[conflictIndex-rf.lastIncluded].Term != conflictTerm {
					break
				}
			}
			conflictIndex++
		} else {
			for i := range args.Entries {
				if args.PrevLogIndex+i+1 > rf.lastIncluded+len(rf.log)-1 {
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.needPersist = true
					break
				}
				if rf.log[args.PrevLogIndex+i+1-rf.lastIncluded].Term != args.Entries[i].Term {
					rf.log = rf.log[:args.PrevLogIndex+i+1-rf.lastIncluded]
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.needPersist = true
					break
				}
			}
			newCommit := args.LeaderCommit
			if args.PrevLogIndex+len(args.Entries) < newCommit {
				newCommit = args.PrevLogIndex + len(args.Entries)
			}
			if newCommit > rf.commitIndex {
				rf.commitIndex = newCommit
				rf.commitUpdate.Signal()
			}
			success = true
		}
		rf.timer.Reset(newElectionTimeOut())
	}
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	reply.Term = term
	reply.Success = success
	reply.ConflictTerm = conflictTerm
	reply.ConflictIndex = conflictIndex
}

func (rf *Raft) sendHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] <= rf.lastIncluded {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncluded,
				LastIncludedTerm:  rf.log[0].Term,
			}
			args.Snapshot = make([]byte, len(rf.snapshot), len(rf.snapshot))
			copy(args.Snapshot, rf.snapshot)
			go rf.sendInstallSnapshot(i, &args, &InstallSnapshotReply{})
		} else {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1-rf.lastIncluded].Term,
				LeaderCommit: rf.commitIndex,
			}
			args.Entries = make([]Entry, rf.lastIncluded+len(rf.log)-rf.nextIndex[i], rf.lastIncluded+len(rf.log)-rf.nextIndex[i])
			copy(args.Entries, rf.log[rf.nextIndex[i]-rf.lastIncluded:])
			go rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.state == leader && reply.Term == rf.currentTerm {
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	} else if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	var term int
	rf.mu.Lock()
	if rf.state == candidate && args.Term == rf.currentTerm {
		rf.state = follower
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}
	if rf.state == follower && args.Term == rf.currentTerm &&
		args.LastIncludedIndex > rf.lastIncluded {
		if args.LastIncludedIndex < rf.lastIncluded+len(rf.log) &&
			rf.log[args.LastIncludedIndex-rf.lastIncluded].Term == args.LastIncludedTerm {
			rf.log = rf.log[args.LastIncludedIndex-rf.lastIncluded:]
		} else {
			rf.log = []Entry{{nil, args.LastIncludedTerm}}
		}
		rf.lastIncluded = args.LastIncludedIndex
		rf.snapshot = args.Snapshot
		rf.needPersist = true
		if rf.commitIndex < rf.lastIncluded {
			rf.commitIndex = rf.lastIncluded
			rf.commitUpdate.Signal()
		}
	}
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	reply.Term = term
}

func (rf *Raft) applyChSender() {
	ch := rf.applyCh
	for !rf.killed() {
		rf.mu.Lock()
		rf.commitUpdate.Wait()
		for rf.lastApplied < rf.commitIndex {
			msgs := rf.makeApplyMessages()
			rf.mu.Unlock()
			for _, msg := range msgs {
				ch <- msg
			}
			rf.mu.Lock()
			if msgs[0].SnapshotValid {
				rf.lastApplied = msgs[0].SnapshotIndex + len(msgs) - 1
			} else {
				rf.lastApplied += len(msgs)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) makeApplyMessages() []ApplyMsg {
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastIncluded {
		msgs = make([]ApplyMsg, rf.commitIndex-rf.lastIncluded+1, rf.commitIndex-rf.lastIncluded+1)
		msgs[0] = ApplyMsg{
			SnapshotValid: true,
			Snapshot:      make([]byte, len(rf.snapshot), len(rf.snapshot)),
			SnapshotTerm:  rf.log[0].Term,
			SnapshotIndex: rf.lastIncluded,
		}
		copy(msgs[0].Snapshot, rf.snapshot)
		for i := rf.lastIncluded + 1; i <= rf.commitIndex; i++ {
			msgs[i-rf.lastIncluded] = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIncluded].Command,
				CommandIndex: i,
			}
		}
	} else {
		msgs = make([]ApplyMsg, rf.commitIndex-rf.lastApplied, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs[i-rf.lastApplied-1] = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIncluded].Command,
				CommandIndex: i,
			}
		}
	}
	return msgs
}
