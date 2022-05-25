package raft

import (
	"math"
	"sort"
	"time"
)

type RequestAppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type RequestAppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// optimization
	Conflict               bool
	ConflictTerm           int
	ConflictTermFirstIndex int
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		if rf.getStatus() != LEADER {
			return
		}

		MyDebug(dLeader, "S%d begin appendEntries LOGS:%v", rf.me, rf.getLogs())

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			prevLogIndex := rf.nextIndex[i] - 1
			if prevLogIndex >= rf.LastIncludedIndex {
				go rf.appendEntries(i)
			} else {
				go rf.appendSnapshot(i)
			}
		}

		time.Sleep(HAERTSBEAT_INTERVAL)
	}
}

func (rf *Raft) appendEntries(server int) {

	var entries []Log
	var prevLogIndex int
	var prevLogTerm int

	rf.mu.Lock()
	prevLogIndex = rf.nextIndex[server] - 1
	if prevLogIndex < 1 {
		prevLogTerm = -1
	} else {
		// prevLogTerm = rf.Logs[prevLogIndex-1].Term
		prevLogTerm = rf.getLog(prevLogIndex).Term
	}

	for i := prevLogIndex + 1; i <= rf.getLogLen(); i++ {
		// entries = append(entries, rf.Logs[i-1])
		entries = append(entries, rf.getLog(i))
	}
	rf.mu.Unlock()

	args := RequestAppendEntriesArgs{
		Term:         rf.getCurrentTerm(),
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := RequestAppendEntriesReply{}

	if rf.getStatus() != LEADER {
		// rf.MyDPrint("Raft服务器#%d 不再是LEADER服务器，取消对#%d发送日志追加消息\n", rf.me, server)
		return
	}

	if len(entries) > 0 {
		MyDebug(dLog, "S%d -> S%d send logs LOGS:%v", rf.me, server, entries)
	} else {
		MyDebug(dLog, "S%d -> S%d heartbeat", rf.me, server)
	}

	ok := rf.sendRequestAppendEntries(server, &args, &reply)
	if !ok {
		MyDebug(dLog, "S%d can't connect S%d", rf.me, server)
		return
	}

	// follower服务器的term比leader要高，取消当前leader身份变为follower
	if reply.Term > rf.getCurrentTerm() {
		MyDebug(dStatus, "S%d -> FOLLOWER - S%d.Term(%d) > currentTerm(%d)", rf.me, server, rf.getCurrentTerm())
		rf.setCurrentTerm(reply.Term)
		rf.persist()
		rf.setStatus(FOLLOWER)
		return
	}

	if reply.Success {

		// 心跳包
		if len(entries) == 0 {
			return
		}

		// 计算出应该提交的日志
		rf.mu.Lock()
		rf.nextIndex[server] = prevLogIndex + len(entries) + 1
		// TODO 也许是我理解错了matchIndex的意思
		rf.matchIndex[server] = prevLogIndex + len(entries)
		MyDebug(dLog, "S%d -> S%d been success append logs LOGS:%v", rf.me, server, entries)
		copyMatch := make([]int, len(rf.matchIndex))
		copy(copyMatch, rf.matchIndex)

		sort.Ints(copyMatch)
		var mid int
		if len(copyMatch)%2 == 0 {
			mid = len(copyMatch)/2 - 1
		} else {
			mid = len(copyMatch) / 2
		}
		rf.commitIndex = copyMatch[mid]
		// rf.MyDPrint("LEADER#%d sorted match:%v\n", rf.me, copyMatch)
		// rf.MyDPrint("LEADER#%d majority match:%d\n", rf.me, majorityMatch)
		// rf.MyDPrint("LEADER#%d rf.commitIndex:%d\n", rf.me, rf.commitIndex)
		// rf.MyDPrint("LEADER#%d rf.logs:%v\n", rf.me, rf.Logs)
		// if rf.commitIndex > rf.lastApplied && rf.Logs[rf.commitIndex-1].Term == rf.CurrentTerm {
		needApplyMsgs := make([]ApplyMsg, 0)
		if rf.commitIndex > rf.lastApplied && rf.getLog(rf.commitIndex).Term == rf.CurrentTerm {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					// Command:      rf.Logs[i-1].Command,
					Command:      rf.getLog(i).Command,
					CommandIndex: i,
				}
				needApplyMsgs = append(needApplyMsgs, applyMsg)
				MyDebug(dCommit, "S%d -> Client MSG:%v", rf.me, applyMsg)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		for _, applyMsg := range needApplyMsgs {
			rf.applyCh <- applyMsg
		}
	} else {
		rf.mu.Lock()
		beforeNextIndex := rf.nextIndex[server]
		// optimized to reduce the number of rejected AppendEntries RPCs.
		if reply.Conflict {
			if reply.ConflictTermFirstIndex == 0 {
				rf.nextIndex[server] = 1
			} else {
				// if rf.Logs[reply.ConflictTermFirstIndex-1].Term == reply.ConflictTerm {
				if rf.getLog(reply.ConflictTermFirstIndex).Term == reply.ConflictTerm {
					rf.nextIndex[server] = reply.ConflictTermFirstIndex + 1
				} else {
					rf.nextIndex[server] = reply.ConflictTermFirstIndex
				}
			}
		} else {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
		}
		MyDebug(dLog, "S%d <- S%d refused append log", rf.me, server)
		MyDebug(dLog, "S%d -> S%d change NextIndex %d -> %d", rf.me, server, beforeNextIndex, rf.nextIndex[server])
		rf.mu.Unlock()
	}
	// rf.mu.Unlock()
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// code for 2A，2B
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		return
	}

	// rf.MyDPrint("Raft服务器#%d 日志追加请求中的CommitIndex为%d 自身的CommitIndex为%d", rf.me, args.LeaderCommit, rf.commitIndex)

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		MyDebug(dLog, "S%d -> S%d refuse log append request - requestTerm(%d) < currentTerm(%d)",
			rf.me, args.LeaderId, args.Term, rf.CurrentTerm)
		return
	}

	rf.heartsbeat = true
	rf.CurrentTerm = args.Term
	if rf.status != FOLLOWER {
		MyDebug(dLog, "S%d <- S%d accept append log", rf.me, args.LeaderId)
		MyDebug(dStatus, "S%d -> FOLLOWER", rf.me)
		rf.status = FOLLOWER
	}

	if args.PrevLogIndex > rf.getLogLen() {
		var conflictTerm int
		var conflictTermFirstIndex int
		if rf.getLogLen() == 0 {
			conflictTerm = -1
			conflictTermFirstIndex = 0
		} else {
			// conflictTerm = rf.Logs[len(rf.Logs)-1].Term
			conflictTerm = rf.getLog(rf.getLogLen()).Term
			for i := rf.LastIncludedIndex; i <= len(rf.Logs); i++ {
				// if rf.Logs[i-1].Term == reply.ConflictTerm {
				if rf.getLog(i).Term == reply.ConflictTerm {
					conflictTermFirstIndex = i
					break
				}
			}
		}
		reply.Conflict = true
		reply.ConflictTerm = conflictTerm
		reply.ConflictTermFirstIndex = conflictTermFirstIndex
		reply.Success = false
		reply.Term = rf.CurrentTerm
		MyDebug(dLog, "S%d -> S%d refuse log append - requests.PrevLogIndex(%d) > logs len(%d)",
			rf.me, args.LeaderId, args.PrevLogIndex, rf.getLogLen())
		return
	}

	// an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	// if args.PrevLogIndex-1 >= 0 && rf.Logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
	if args.PrevLogIndex-1 >= 0 && rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		reply.ConflictTerm = rf.getLog(args.PrevLogIndex).Term
		for i := rf.LastIncludedIndex; i <= rf.getLogLen(); i++ {
			// if rf.Logs[i-1].Term == reply.ConflictTerm {
			if rf.getLog(i).Term == reply.ConflictTerm {
				reply.ConflictTermFirstIndex = i
				break
			}
		}
		// rf.Logs = rf.Logs[:args.PrevLogIndex-1]
		rf.Logs = rf.Logs[:args.PrevLogIndex-1-rf.LastIncludedIndex]
		reply.Success = false
		reply.Term = rf.CurrentTerm
		MyDebug(dLog, "S%d -> S%d refuse log append - log conflict", rf.me, args.LeaderId)
		return
	}

	// 空entries的心跳包
	if len(args.Entries) == 0 {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		reply.Success = true
		reply.Term = rf.CurrentTerm
	} else {
		// Append any new entries not already in the log
		// rf.Logs = rf.Logs[:args.PrevLogIndex]
		rf.Logs = rf.Logs[:args.PrevLogIndex-rf.LastIncludedIndex]
		rf.Logs = append(rf.Logs, args.Entries...)
		reply.Success = true
		reply.Term = rf.CurrentTerm
		MyDebug(dLog, "S%d <- S%d accept log append LOG:%v", rf.me, args.LeaderId, args.Entries)
	}

	// check commitIndex
	if args.LeaderCommit > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLogLen())))
		MyDebug(dCommit, "S%d commidIndex %d -> %d LOGS:%v", rf.me, originCommitIndex, rf.commitIndex, rf.Logs)
		needApplyMsgs := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				// Command:      rf.Logs[i-1].Command,
				Command:      rf.getLog(i).Command,
				CommandIndex: i,
			}
			needApplyMsgs = append(needApplyMsgs, applyMsg)
			MyDebug(dCommit, "S%d -> Client MSG:%v", rf.me, applyMsg)
		}
		rf.lastApplied = rf.commitIndex

		// 为了不和snapshot发生死锁
		rf.mu.Unlock()
		for _, applyMsg := range needApplyMsgs {
			rf.applyCh <- applyMsg
		}
		rf.mu.Lock()
	}

}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}
