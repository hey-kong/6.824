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

const (
	Follower = iota
	Candidate
	Leader
)

const (
	// the tester limits to 10 heartbeats per second
	HEARTBEAT_TIME  = time.Duration(150) * time.Millisecond
	ELEC_TIME_LOWER = time.Duration(450) * time.Millisecond
	ELEC_TIME_UPPER = time.Duration(600) * time.Millisecond
)

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

type Log struct {
	Command interface{}
	Index   int
	Term    int
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
	state          int
	votes          int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	// 所有服务器上持久存在的
	currentTerm int
	votedFor    int
	logs        []Log
	// 所有服务器上经常变的
	commitIndex int // 已经被大多数节点保存的日志的位置
	lastApplied int // 被应用到状态机的日志的位置
	// 在领导人里经常变的(选举后重新初始化)
	nextIndex  []int // 每个peer都有一个值，是leader要发送给其他peer的下一个日志索引
	matchIndex []int // 每个peer都有一个值，是leader收到的其他peer已经复制给它的日志的最高索引值

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
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
	var logs []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Errorf("[readPersist]: Decode Error!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// 已经投过票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		rf.persist()
	}

	lastLogIndex := len(rf.logs) - 1
	if args.LastLogTerm < rf.logs[lastLogIndex].Term ||
		(args.LastLogTerm == rf.logs[lastLogIndex].Term && args.LastLogIndex < lastLogIndex) {
		// 比候选人的日志新
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	// 投票后需要重置选举时间
	rf.resetElectionTimer()
}

type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// optimization: for "unreliable" passed
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 收到Leader心跳包后需要重置选举时间
	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		rf.persist()
	}

	if len(rf.logs)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term

		conflictIndex := args.PrevLogIndex
		for rf.logs[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	unmatchedStart := -1
	for i := range args.Entries {
		if len(rf.logs) == args.PrevLogIndex+1+i ||
			rf.logs[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
			unmatchedStart = i
			break
		}
	}
	if unmatchedStart != -1 {
		rf.logs = rf.logs[:args.PrevLogIndex+1+unmatchedStart]
		rf.logs = append(rf.logs, args.Entries[unmatchedStart:]...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := len(rf.logs) - 1
		if args.LeaderCommit <= lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
		rf.applyTo(rf.commitIndex)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs, Log{Command: command, Term: term, Index: index})
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		//fmt.Printf("%d start agreement on command %d on index %d(Term: %d)\n", rf.me, command.(int), index, rf.currentTerm)
		rf.broadcastHeartbeat()
		rf.mu.Unlock()
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
	rf.votes = 0
	// 所有服务器上持久存在的
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 1)
	// 所有服务器上经常变的
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 在领导人里经常变的(选举后重新初始化)
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	go rf.raftLoop()
	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	return rf
}

func randTimeDuration(lower, upper time.Duration) time.Duration {
	d := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(d) * time.Nanosecond
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(randTimeDuration(ELEC_TIME_LOWER, ELEC_TIME_UPPER))
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Reset(HEARTBEAT_TIME)
}

func (rf *Raft) raftLoop() {
	rf.mu.Lock()
	rf.electionTimer = time.NewTimer(randTimeDuration(ELEC_TIME_LOWER, ELEC_TIME_UPPER))
	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_TIME)
	rf.mu.Unlock()
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Follower {
				rf.convertTo(Candidate)
			} else {
				rf.startElection()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcastHeartbeat()
				rf.resetHeartbeatTimer()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) convertTo(state int) {
	if rf.state == state {
		return
	}

	rf.state = state
	switch state {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.votedFor = -1
		rf.resetElectionTimer()
	case Candidate:
		rf.startElection()
	case Leader:
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		rf.resetHeartbeatTimer()
	default:
		fmt.Printf("Warning: invaid state %d, do nothing\n", state)
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.votes = 1
	rf.resetElectionTimer()

	rf.broadcastVoteReq()
}

func (rf *Raft) broadcastVoteReq() {
	lastLogIndex := len(rf.logs) - 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.logs[lastLogIndex].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				if reply.VoteGranted && rf.state == Candidate {
					rf.votes++
					if rf.votes > len(rf.peers)/2 {
						rf.convertTo(Leader)
						//fmt.Printf("Term %d: %d is the new leader\n", rf.currentTerm, rf.me)
					}
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.convertTo(Follower)
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			entries := make([]Log, len(rf.logs[prevLogIndex+1:]))
			copy(entries, rf.logs[prevLogIndex+1:])
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= i {
								count += 1
							}
						}

						if count > len(rf.peers)/2 {
							rf.commitIndex = i
							rf.applyTo(rf.commitIndex)
							break
						}
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						rf.persist()
					} else {
						rf.nextIndex[server] = reply.ConflictIndex

						if reply.ConflictTerm != -1 {
							for i := args.PrevLogIndex; i >= 1; i-- {
								if rf.logs[i-1].Term == reply.ConflictTerm {
									rf.nextIndex[server] = i
									break
								}
							}
						}
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

// applyTo should be called after commitIndex updated
func (rf *Raft) applyTo(commitIndex int) {
	if commitIndex > rf.lastApplied {
		go func(start int, entries []Log) {
			for i, entry := range entries {
				var applyMsg ApplyMsg
				applyMsg.CommandValid = true
				applyMsg.Command = entry.Command
				applyMsg.CommandIndex = start + i
				rf.applyCh <- applyMsg
				rf.mu.Lock()
				rf.lastApplied = applyMsg.CommandIndex
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, rf.logs[rf.lastApplied+1:rf.commitIndex+1])
	}
}
