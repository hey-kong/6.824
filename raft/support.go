package raft

import "sort"

func send(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) getLog(i int) Log {
	return rf.logs[i-rf.lastIncludedIndex]
}

func (rf *Raft) logLen() int {
	return len(rf.logs) + rf.lastIncludedIndex
}

func (rf *Raft) getPrevLogIdx(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLog(prevLogIdx).Term
}

func (rf *Raft) getLastLogIdx() int {
	return rf.logLen() - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIdx()
	if idx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLog(idx).Term
}

func (rf *Raft) updateMatchIndex(server int, matchIdx int) {
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.Commit()
	}
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
