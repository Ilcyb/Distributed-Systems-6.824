package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"github.com/sasha-s/go-deadlock"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	CurrentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VoteFor     int   // candidateId that received vote in current term (or null if none)
	Logs        []Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders and all of it reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// 是否收到leader的心跳包
	heartsbeat bool // 在election timeout内有没有收到过来心跳包
	status     int  // 目前自己的状态
	applyCh    chan ApplyMsg

	// snapshot
	LastIncludedIndex int
	LastIncludedTerm  int
}

type Log struct {
	Command interface{}
	Term    int
}

const (
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3
)

const (
	HAERTSBEAT_INTERVAL     = 150 * time.Millisecond
	ELECTION_FAILED_TIMEOUT = 1000 * time.Millisecond
)

const (
	ELECTION_SUCCESS   = 1
	ELECTION_FAILED    = -1
	ELECTION_NO_RESULT = 0
	RECONVERT_FOLLOWER = 2
	ELECTION_TIMEOUT   = 3
	KILLED_ABORT       = 4
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.getCurrentTerm(), rf.getStatus() == LEADER
}

func (rf *Raft) getEncodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	rf.mu.Unlock()
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	data := rf.getEncodeState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("Raft服务器#%d 读取持久化数据失败", rf.me)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VoteFor = voteFor
		rf.Logs = logs
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	// rf.MyDPrint("Raft服务器#%d 收到Start请求 %v", rf.me, command)
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if rf.killed() || rf.status != LEADER {
		return -1, -1, false
	}

	MyDebug(dLeader, "S%d get command COMMAND:%v", rf.me, command)
	term := rf.CurrentTerm
	newLog := Log{
		Term:    term,
		Command: command,
	}
	rf.Logs = append(rf.Logs, newLog)
	// index := len(rf.Logs)
	index := rf.getLogLen()
	rf.matchIndex[rf.me] = index

	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rand.Seed(int64(rf.me))
		// rand.Seed(time.Now().UnixNano())
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)+150))

		MyDebug(dInfo, "S%d STATUS:%s Logs:%v LastIncIdx:%d Term:%d ComIdx%d AplIdx:%d",
			rf.me, statusMap[rf.status], rf.Logs, rf.LastIncludedIndex, rf.CurrentTerm, rf.commitIndex, rf.lastApplied)

		// 如果该server是leader的话则不需要等待心跳包
		if rf.getStatus() == LEADER {
			continue
		}

		if !rf.getHeartsbeat() {
			// 超时未收到来自leader的心跳，状态变为candidate
			rf.setStatus(CANDIDATE)
			MyDebug(dStatus, "S%d -> CANDIDATE - heartbeat timeout", rf.me)
		}

		if rf.getStatus() == CANDIDATE {
			rf.election()
		} else {
			rf.setHeartsbeat(false)
		}
	}
}

func (rf *Raft) setStatus(status int) {
	rf.mu.Lock()
	rf.status = status
	rf.mu.Unlock()
}

func (rf *Raft) getStatus() int {
	rf.mu.Lock()
	returnVal := rf.status
	rf.mu.Unlock()
	return returnVal
}

func (rf *Raft) setCurrentTerm(currentTerm int) {
	rf.mu.Lock()
	rf.CurrentTerm = currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	returnVal := rf.CurrentTerm
	rf.mu.Unlock()
	return returnVal
}

func (rf *Raft) setHeartsbeat(heartsbeat bool) {
	rf.mu.Lock()
	rf.heartsbeat = heartsbeat
	rf.mu.Unlock()
}

func (rf *Raft) getHeartsbeat() bool {
	rf.mu.Lock()
	returnVal := rf.heartsbeat
	rf.mu.Unlock()
	return returnVal
}

func (rf *Raft) setVoteFor(vote int) {
	rf.mu.Lock()
	rf.VoteFor = vote
	rf.mu.Unlock()
}

func (rf *Raft) getVoteFor() int {
	rf.mu.Lock()
	returnVal := rf.VoteFor
	rf.mu.Unlock()
	return returnVal
}

func (rf *Raft) getLogs() []Log {
	rf.mu.Lock()
	returnVal := rf.Logs
	rf.mu.Unlock()
	return returnVal
}

func (rf *Raft) getLog(index int) Log {
	if index == rf.LastIncludedIndex {
		return Log{Term: rf.LastIncludedTerm}
	}

	logLen := len(rf.Logs) + rf.LastIncludedIndex

	if index == -1 {
		if logLen == 0 {
			return Log{Term: -1}
		}
		return rf.Logs[len(rf.Logs)-1]
	}

	if logLen == 0 || index < 1 || index > logLen {
		return Log{Term: -1}
	}

	MyDebug(dTrace, "S%d logTotalLen:%d len(logs):%d X:%d requestIndex:%d index-rf.XIndex-1:%d",
		rf.me, logLen, len(rf.Logs), rf.LastIncludedIndex, index, index-rf.LastIncludedIndex-1)
	return rf.Logs[index-rf.LastIncludedIndex-1]
}

func (rf *Raft) setLog(index int, log Log) {
	if index <= rf.LastIncludedIndex {
		MyDebug(dError, "S%d - set log of campacted logs SET_INDEX:%d XINDEX:%d", rf.me, index, rf.LastIncludedIndex)
		return
	}

	rf.Logs[index-rf.LastIncludedIndex-1] = log
}

func (rf *Raft) getLogLen() int {
	return rf.LastIncludedIndex + len(rf.Logs)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.Logs = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartsbeat = false
	rf.status = FOLLOWER
	rf.applyCh = applyCh
	rf.VoteFor = -1
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.LastIncludedIndex > 0 {
		rf.lastApplied = rf.LastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
