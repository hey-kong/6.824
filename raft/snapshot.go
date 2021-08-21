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
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// Offset         int    // byte offset where chunk is positioned in the snapshot file
	// Done           bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { //Reply immediately if term < currentTerm
		return
	}
	if args.Term > rf.currentTerm { //all server rule 1 If RPC request or response contains term T > currentTerm:
		rf.beFollower(args.Term) // set currentTerm = T, convert to follower (§5.1)
	}
	send(rf.appendLogCh)                                //If election timeout elapses without receiving AppendEntries RPC from current leader
	if args.LastIncludedIndex <= rf.lastIncludedIndex { // discard any existing or partial snapshot with a smaller index
		return
	}
	applyMsg := ApplyMsg{CommandValid: false, SnapShot: args.Data}
	//If existing log entry has same index and term as snapshot’s last included entry,retain log entries following it and reply
	if args.LastIncludedIndex < rf.logLen()-1 {
		rf.logs = append(make([]Log, 0), rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	} else { //7. Discard the entire log
		rf.logs = []Log{{args.LastIncludedTerm, nil}}
	}
	//Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.persistWithSnapShot(args.Data)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	if rf.lastApplied > rf.lastIncludedIndex {
		return
	} //snapshot is older than kvserver's db, so reply immediately
	rf.applyCh <- applyMsg
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
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		LeaderId:          rf.me,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ret := rf.sendInstallSnapshot(peer, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ret || rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}

	// update succeeded
	rf.updateMatchIndex(peer, rf.lastIncludedIndex)
}

func (rf *Raft) DoSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// lock competition may delay snapshot call, check it,
	// otherwise rf.logAt(index) may out of bounds
	if index <= rf.lastIncludedIndex {
		return
	}

	rf.logs = append(make([]Log, 0), rf.logs[index-rf.lastIncludedIndex:]...)
	// update new lastIncludedIndex and lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.getLog(index).Term
	// save snapshot
	rf.persistStatesAndSnapshot(snapshot)
}
