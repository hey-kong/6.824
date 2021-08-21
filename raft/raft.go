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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type State int

const (
	Follower  State = iota // value --> 0
	Candidate              // value --> 1
	Leader                 // value --> 2
)

const NULL int = -1

type Log struct {
	Term    int
	Command interface{}
}

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
	SnapShot     []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	state       State
	votedFor    int
	logs        []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	//channel
	applyCh chan ApplyMsg // from Make()
	killCh  chan bool     //for Kill()

	//handle rpc
	voteCh      chan bool
	appendLogCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

func (rf *Raft) persistWithSnapShot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		fmt.Errorf("[readPersist]: Decode Error!\n")
	} else {
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor, rf.logs = currentTerm, voteFor, logs
		rf.lastIncludedTerm, rf.lastIncludedIndex = lastIncludedTerm, lastIncludedIndex
		rf.commitIndex, rf.lastApplied = rf.lastIncludedIndex, rf.lastIncludedIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIdx() + 1
	}
}

func (rf *Raft) beCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	go rf.broadcastVoteReq()
}

func (rf *Raft) beFollower(term int) {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
	rf.persist()
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		index = rf.getLastLogIdx() + 1
		newLog := Log{
			rf.currentTerm,
			command,
		}
		rf.logs = append(rf.logs, newLog)
		rf.persist()
		rf.broadcastHeartbeat()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	send(rf.killCh)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.logs = make([]Log, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.voteCh = make(chan bool, 1)
	rf.appendLogCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.service()
	return rf
}

func (rf *Raft) service() {
	for {
		select {
		case <-rf.killCh:
			return
		default:
		}

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		electionTime := time.Duration(rand.Intn(200)+300) * time.Millisecond
		heartbeatTime := time.Duration(100) * time.Millisecond
		switch state {
		case Follower, Candidate:
			select {
			case <-rf.voteCh:
			case <-rf.appendLogCh:
			case <-time.After(electionTime):
				rf.mu.Lock()
				rf.beCandidate()
				rf.mu.Unlock()
			}
		case Leader:
			time.Sleep(heartbeatTime)
			rf.broadcastHeartbeat()
		}
	}
}
