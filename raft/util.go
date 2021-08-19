package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = 0

func (rf *Raft) String() string {
	roleStr := ""
	switch rf.currentRole {
	case RoleLeader:
		roleStr = "Leader"
	case RoleFollower:
		roleStr = "Follower"
	case RoleCandidate:
		roleStr = "Candidate"
	}
	s := fmt.Sprintf("[%d(%s) T%2d V%2d]", rf.me, roleStr, rf.currentTerm, rf.votedFor)
	return s
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
