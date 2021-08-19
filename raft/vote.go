package raft

import (
	"sync"
	"sync/atomic"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
	defer rf.persist()

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.resetTerm(args.Term, NullPeer)
	}

	// voted already
	if rf.votedFor != NullPeer && rf.votedFor != args.CandidateID {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// not up-to-date
	if rf.lastTerm() > args.LastLogTerm ||
		(rf.lastTerm() == args.LastLogTerm &&
			rf.lastIndex() > args.LastLogIndex) {
		return
	}

	DPrintf("%s Vote granted to %d", rf, args.CandidateID)
	reply.VoteGranted = true
	rf.resetTerm(args.Term, args.CandidateID)
	rf.granted <- args.CandidateID
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentRole != RoleCandidate ||
		rf.currentTerm != args.Term {
		return false
	}

	if reply.Term > rf.currentTerm {
		defer rf.persist()
		rf.resetTerm(reply.Term, NullPeer)
		return false
	}
	return true
}

func (rf *Raft) broadcastVoteReq() chan bool {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	DPrintf("%s start voting", rf)
	rf.persist()
	rf.mu.Unlock()

	var voteCount, half int32
	voteCount = 1
	half = int32(len(rf.peers) / 2)
	var wg sync.WaitGroup
	isLeader := make(chan bool)
	for i := range rf.peers {
		if i != rf.me {
			wg.Add(1)
			go func(peer int) {
				defer wg.Done()
				var reply RequestVoteReply
				if ok := rf.sendRequestVote(peer, args, &reply); !ok {
					return
				}

				if reply.VoteGranted {
					if atomic.AddInt32(&voteCount, 1) > half {
						isLeader <- true
						DPrintf("%s End voting", rf)
						return
					}
				}
			}(i)
		}
	}
	return isLeader
}
