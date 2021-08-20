package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = rf.logLength()

	// 1. reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.resetTerm(args.Term, NullPeer)
	}

	rf.heartbeat <- args.LeaderID

	// 2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.lastIndex() {
		return
	}

	// check terms
	if args.PrevLogIndex > rf.lastIncludedIndex && rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		for i := rf.logBegin(); i < rf.logEnd(); i++ {
			if rf.logAt(i).Term == rf.logAt(args.PrevLogIndex).Term {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// sometimes args.PrevLogIndex have lower index. Skip and wait until sendSnapshot finish
	defer func() { recover() }()

	// 3. if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all follow it (§5.3)
	matchedLogs := rf.logRange(rf.logBegin(), args.PrevLogIndex+1)
	mayConflictedLogs := rf.logRange(args.PrevLogIndex+1, rf.logEnd())

	conflicted := false
	for i := 0; i < len(mayConflictedLogs) && i < len(args.Entries); i++ {
		if mayConflictedLogs[i].Term != args.Entries[i].Term {
			conflicted = true
			break
		}
	}

	if len(args.Entries) > len(mayConflictedLogs) {
		conflicted = true
	}

	// append any new entries not already in the log
	if conflicted {
		rf.logs = append(matchedLogs, args.Entries...)
	}

	reply.Success = true
	reply.ConflictIndex = args.PrevLogIndex

	// 4. If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of the last log entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastIndex()
		}
		go rf.Commit()
	}

	rf.resetTerm(args.Term, args.LeaderID)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentRole != RoleLeader || rf.currentTerm != args.Term {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.resetTerm(reply.Term, NullPeer)
		return false
	}
	return true
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("%s send logs: %v", rf, rf.logs)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentRole == RoleLeader {
		rf.matchIndex[rf.me] = rf.lastIndex()
		for i := range rf.peers {
			if i != rf.me {
				index := rf.nextIndex[i] - 1
				// have no logs previous to rf.lastIncludedIndex => send snapshot
				if index < rf.lastIncludedIndex {
					go rf.sendSnapshot(i)
					continue
				}

				term := -1
				entries := []LogEntry{}
				if index < rf.logLength() {
					term = rf.logAt(index).Term
					entries = make([]LogEntry, rf.logLength()-index-1)
					copy(entries, rf.logRange(index+1, rf.logEnd()))
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					LeaderCommit: rf.commitIndex,
					PrevLogIndex: index,
					PrevLogTerm:  term,
					Entries:      entries,
				}

				go func(peer int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					if ok := rf.sendAppendEntries(peer, &args, &reply); !ok {
						return
					}

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Success {
						rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
					} else {
						// failed, mark to unconflicted index
						rf.nextIndex[peer] = reply.ConflictIndex
					}

					// If there exists an N such that N > commitIndex, a majority
					// of matchIndex[i] ≥ N, and log[N].term == currentTerm:set commitIndex = N (§5.3, §5.4)
					for N := rf.lastIndex(); N > rf.commitIndex; N-- {
						count := 0
						for p := range rf.peers {
							if rf.matchIndex[p] >= N {
								count++
							}
						}

						if count > len(rf.peers)/2 && rf.logAt(N).Term == rf.currentTerm {
							rf.commitIndex = N
							go rf.Commit()
							break
						}
					}
				}(i, args)
			}
		}
	}
}

// Commit should be called after commitIndex updated
func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logAt(index).Command,
			CommandIndex: index,
		}
	}
	rf.lastApplied = rf.commitIndex
}
