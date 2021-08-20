package raft

import (
	"bytes"

	"6.824/labgob"
)

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// check no higher term
	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.resetTerm(args.Term, NullPeer)
	}
	rf.heartbeat <- args.LeaderId

	// check have the latest last include index
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset. Write into go objects,
	// no need of offset
	// 4. Reply and wait for more data chunks if done is false
	if !args.Done {
		return
	}

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	// 6. If existing log entry has the same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	// 7. Discard the entire log
	if rf.lastIncludedIndex <= args.LastIncludedIndex {
		rf.logs = []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	} else {
		rf.logs = rf.logRange(args.LastIncludedIndex, rf.logEnd())
	}

	// update local index and term
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.persistStatesAndSnapshot(args.Data)

	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}

	// 8. reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.applyCh <- ApplyMsg{
		CommandIndex: -1,
		Command:      args.Data,
		CommandValid: false, // reset the state machine
	}
}

func (rf *Raft) persistStatesAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(peer int) {
	rf.mu.Lock()
	role := rf.currentRole
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	rf.mu.Unlock()

	if role != RoleLeader {
		return
	}

	var reply InstallSnapshotReply
	if ok := rf.sendInstallSnapshot(peer, &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.resetTerm(reply.Term, NullPeer)
		return
	}

	if rf.currentRole != RoleLeader ||
		reply.Term < rf.currentTerm /* not the same term */ {
		return
	}

	// update succeeded
	rf.nextIndex[peer] = args.LastIncludedIndex + 1
	rf.matchIndex[peer] = args.LastIncludedIndex
}

func (rf *Raft) StartSnapshotOn(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// lock competition may delay snapshot call, check it,
	// otherwise rf.logAt(index) may out of bounds
	if index <= rf.lastIncludedIndex {
		return
	}

	rf.logs = append(make([]LogEntry, 0), rf.logRange(index, rf.logEnd())...)
	// update new lastIncludedIndex and lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logAt(index).Term
	// save snapshot
	rf.persistStatesAndSnapshot(snapshot)
}
