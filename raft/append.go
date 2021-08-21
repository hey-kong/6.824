package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Log
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	send(rf.appendCh)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// reply false if logs doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	prevLogIndexTerm := -1
	logSize := rf.logLen()
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < rf.logLen() {
		prevLogIndexTerm = rf.getLog(args.PrevLogIndex).Term
	}

	// check terms
	if prevLogIndexTerm != args.PrevLogTerm {
		reply.ConflictIndex = logSize
		if prevLogIndexTerm == -1 {
			// return with conflictIndex = len(logs) and conflictTerm = -1
		} else {
			reply.ConflictTerm = prevLogIndexTerm
			for i := rf.lastIncludedIndex; i < logSize; i++ {
				if rf.getLog(i).Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all follow it (§5.3)
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index < logSize {
			if rf.getLog(index).Term == args.Entries[i].Term {
				continue
			} else {
				rf.logs = rf.logs[:index-rf.lastIncludedIndex]
			}
		}
		// append any new entries not already in the logs
		rf.logs = append(rf.logs, args.Entries[i:]...)
		rf.persist()
		break
	}

	// if LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of the last logs entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIdx())
		rf.Commit()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("%d send logs: %v", rf.me, rf.logs)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[peer]-rf.lastIncludedIndex < 1 {
					rf.sendSnapshot(peer)
					return
				}

				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					rf.getPrevLogIdx(peer),
					rf.getPrevLogTerm(peer),
					rf.commitIndex,
					append(make([]Log, 0), rf.logs[rf.nextIndex[peer]-rf.lastIncludedIndex:]...),
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &args, reply)
				rf.mu.Lock()
				if !ok || rf.state != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				// update nextIndex and matchIndex for follower
				if reply.Success {
					rf.updateMatchIndex(peer, args.PrevLogIndex+len(args.Entries))
					rf.mu.Unlock()
					return
				} else {
					// decrement nextIndex and retry
					index := reply.ConflictIndex
					if reply.ConflictTerm != NULL {
						logSize := rf.logLen()
						for i := rf.lastIncludedIndex; i < logSize; i++ {
							if rf.getLog(i).Term != reply.ConflictTerm {
								continue
							}
							for i < logSize && rf.getLog(i).Term == reply.ConflictTerm {
								i++
							}
							index = i
						}
					}
					rf.nextIndex[peer] = min(rf.logLen(), index)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// Commit should be called after commitIndex updated
func (rf *Raft) Commit() {
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		log := rf.getLog(rf.lastApplied)
		applyMsg := ApplyMsg{true, log.Command, rf.lastApplied, nil}
		rf.applyCh <- applyMsg
	}
}
