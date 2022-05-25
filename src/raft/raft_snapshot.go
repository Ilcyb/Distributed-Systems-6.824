package raft

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	if index > rf.commitIndex || index <= rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	MyDebug(dSnap, "S%d snapshot INDEX:%d LAST_XINDEX:%d", rf.me, index, rf.LastIncludedIndex)
	lastIncludeIndex := index - rf.LastIncludedIndex
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = rf.Logs[lastIncludeIndex-1].Term
	// snapshot 不包括splitIndex所指的log 如果需要包括的话要把 indexLag-1 改为 indexLag
	rf.compactLogs(lastIncludeIndex)
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.mu.Unlock()

	rf.persister.SaveStateAndSnapshot(rf.getEncodeState(), snapshot)
}

func (rf *Raft) compactLogs(lastIncludeIndex int) {
	newLogs := make([]Log, len(rf.Logs)-lastIncludeIndex)
	copy(newLogs, rf.Logs[lastIncludeIndex:])
	rf.Logs = newLogs
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		MyDebug(dSnap, "S%d refuse snapshot from S%d - S%d.Term(%d) > S%d.Term(%d)",
			rf.me, args.LeaderId, rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}

	if rf.status == LEADER && args.Term <= rf.CurrentTerm {
		MyDebug(dSnap, "S%d refuse snapshot from S%d - S%d is LEADER and currentTerm(%d) >= request.Term(%d)",
			rf.me, args.LeaderId, rf.me, rf.CurrentTerm, args.Term)
		rf.mu.Unlock()
		return
	}

	// like heartbeat
	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm
	rf.heartsbeat = true
	if rf.status != FOLLOWER {
		rf.status = FOLLOWER
		MyDebug(dStatus, "S%d -> FOLLOWER - receive valid InstallSnapshot")
	}

	//  discard any existing or partial snapshot with a smaller index
	if args.LastIncludedIndex < rf.getLogLen() {
		MyDebug(dSnap, "S%d refuse snapshot from S%d - args.LastIncludedIndex(%d) <= rf.logLen(%d)",
			rf.me, args.LeaderId, args.LastIncludedIndex, rf.getLogLen())
		rf.mu.Unlock()
		return
	}

	// If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	for index, log := range rf.Logs {
		if rf.LastIncludedIndex+index+1 == args.LastIncludedIndex && log.Term == args.Term {
			rf.compactLogs(index + 1)
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex
			rf.LastIncludedIndex = args.LastIncludedIndex
			rf.LastIncludedTerm = args.LastIncludedTerm
			MyDebug(dSnap, "S%d accept snapshot from S%d and compact logs - LIIndex:%d",
				rf.me, args.LeaderId, rf.LastIncludedIndex)
			rf.mu.Unlock()
			rf.persister.SaveStateAndSnapshot(rf.getEncodeState(), args.Data)
			return
		}
	}

	rf.Logs = make([]Log, 0)
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	MyDebug(dSnap, "S%d accept snapshot from S%d - LIIndex:%d",
		rf.me, args.LeaderId, rf.LastIncludedIndex)
	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(rf.getEncodeState(), args.Data)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) appendSnapshot(server int) {
	snapshot := rf.persister.ReadSnapshot()
	args := InstallSnapshotArgs{
		Term:              rf.getCurrentTerm(),
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              snapshot,
	}
	reply := InstallSnapshotReply{}

	if rf.getStatus() != LEADER {
		return
	}

	MyDebug(dLog, "S%d -> S%d snapshot LIIndex:%d", rf.me, server, args.LastIncludedIndex)
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		MyDebug(dSnap, "S%d can't connect S%d", rf.me, server)
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

	rf.mu.Lock()
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.mu.Unlock()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
