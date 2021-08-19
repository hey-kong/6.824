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
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

const (
	NullPeer      = -1
	RoleFollower  = 0
	RoleCandidate = 1
	RoleLeader    = 2
)

type LogEntry struct {
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
	currentRole Role
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	applyCh   chan ApplyMsg
	applyChMu sync.Mutex
	heartbeat chan int
	running   bool
	granted   chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentRole == RoleLeader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Errorf("[readPersist]: Decode Error!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
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
	if rf.currentRole == RoleLeader {
		defer rf.persist()
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		return rf.lastIndex(), rf.currentTerm, true
	} else {
		DPrintf("%s Start %v Failed", rf, command)
		return 0, 0, false
	}
}

func (rf *Raft) resetTerm(higherTerm int, peer int) {
	rf.currentRole = RoleFollower
	rf.currentTerm = higherTerm
	rf.votedFor = peer
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.running = false
}

func (rf *Raft) service() {
	for {
		rf.mu.Lock()
		role := rf.currentRole
		if !rf.running {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		switch role {
		case RoleFollower:
			select {
			case <-rf.granted:
				// pass
				DPrintf("%s receive granted", rf)
			case <-rf.heartbeat:
				// pass
				DPrintf("%s receive heartbeat", rf)
			case <-time.After(randElectTime()):
				rf.mu.Lock()
				DPrintf("%s no heartbeat, upgrade to Candidate", rf)
				rf.currentRole = RoleCandidate
				rf.mu.Unlock()
			}
		case RoleCandidate:
			select {
			case <-rf.broadcastVoteReq():
				DPrintf("%s Upgrade to Leader", rf)
				rf.broadcastHeartbeat()
				rf.mu.Lock()
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.logLength()
				}
				rf.currentRole = RoleLeader
				rf.mu.Unlock()
			case <-rf.granted:
				rf.mu.Lock()
				rf.currentRole = RoleFollower
				rf.mu.Unlock()
			case <-rf.heartbeat:
				rf.mu.Lock()
				rf.currentRole = RoleFollower
				rf.mu.Unlock()
			case <-time.After(randElectTime()): // cannot get enough vote
				// pass
			}
		case RoleLeader:
			rf.broadcastHeartbeat()
			time.Sleep(50 * time.Millisecond)
		}
	}
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
	rf.resetTerm(0, NullPeer)
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.applyCh = applyCh
	rf.heartbeat = make(chan int, 100)
	rf.granted = make(chan int, 100)
	rf.running = true
	rf.logs = []LogEntry{{Term: 0, Command: nil}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	go rf.service()
	return rf
}
