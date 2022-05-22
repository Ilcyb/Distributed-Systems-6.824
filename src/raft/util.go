package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var statusMap = map[int]string{
	CANDIDATE: "CANDIDATE",
	LEADER:    "LEADER",
	FOLLOWER:  "FOLLOWER",
}

func (rf *Raft) MyDPrint(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("("+statusMap[rf.status]+") "+format, a...)
	}
	return
}
