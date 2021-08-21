package raft

import (
	"sync/atomic"
)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if (args.Term < rf.currentTerm) || (rf.votedFor != NULL && rf.votedFor != args.CandidateId) {
		// reply false if term < currentTerm (§5.1)
		// if votedFor is not null and not candidateId, voted already
	} else if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIdx()) {
		// not up-to-date
	} else {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Follower
		rf.persist()
		send(rf.voteCh)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastVoteReq() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIdx(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	votes := int32(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(idx int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(idx, &args, reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					return
				}
				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}
				if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
					rf.beLeader()
					rf.broadcastHeartbeat()
					send(rf.voteCh)
				}
			}
		}(i)
	}
}
