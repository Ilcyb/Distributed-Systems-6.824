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
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	rf.mu.Unlock()
	data := w.Bytes()
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
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Printf("Raft服务器#%d 读取持久化数据失败", rf.me)
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VoteFor = voteFor
		rf.Logs = logs
		rf.mu.Unlock()
	}
}

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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// code for 2A
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// code for 2A
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		MyDebug(dVote, "S%d refuse vote S%d - requestTerm(%d) < currentTerm(%d) ", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
		return
	}

	//  the RPC includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	var lastLog Log
	var lastLogIndex int
	if len(rf.Logs)-1 < 0 {
		lastLog = Log{
			Term: -1,
		}
		lastLogIndex = 0
	} else {
		lastLog = rf.Logs[len(rf.Logs)-1]
		lastLogIndex = len(rf.Logs)
	}
	if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		MyDebug(dVote, "S%d refuse vote S%d - request lastLogIndex(%d) < lastLogIndex(%d) ",
			rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.status = FOLLOWER
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.heartsbeat = true
		MyDebug(dVote, "S%d vote for S%d - requestTerm(%d) > currentTerm(%d)", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
		MyDebug(dStatus, "S%d -> FOLLOWER")
		return
	}

	if rf.status == LEADER {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		MyDebug(dVote, "S%d refuse vote S%d - is LEADER", rf.me, args.CandidateId)
		return
	}

	if rf.VoteFor == -1 && args.Term == rf.CurrentTerm && args.LastLogIndex >= rf.commitIndex {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.heartsbeat = true
		MyDebug(dVote, "S%d refuse vote S%d - request lastLogIndex(%d) < lastLogIndex(%d)",
			rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	index := len(rf.Logs)
	rf.matchIndex[rf.me] = len(rf.Logs)

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

func (rf *Raft) election() {
	rf.setCurrentTerm(rf.getCurrentTerm() + 1)
	rf.setVoteFor(rf.me)
	rf.persist()
	voteSuccessChan := make(chan int, len(rf.peers))
	resultCh := make(chan int)
	MyDebug(dVote, "S%d request election", rf.me)
	go rf.sendAllElectionRequests(voteSuccessChan, resultCh)
	voteNum := 1
	go func() {
		for {
			// 长时间goroutine操作要判断 killed
			if rf.killed() {
				select {
				case resultCh <- KILLED_ABORT:
				default:
				}
				return
			}

			select {
			case <-voteSuccessChan:
				voteNum++
				if voteNum > (len(rf.peers) / 2) {
					// 选举成功
					select {
					case resultCh <- ELECTION_SUCCESS:
					default:
					}
					goto EXIT
				}
			case <-time.After(ELECTION_FAILED_TIMEOUT): // TODO 确认选举的超时时间是多长
				select {
				case resultCh <- ELECTION_TIMEOUT:
				default:
				}
				goto EXIT
			}
		}
	EXIT:
	}()

	result := <-resultCh
	// 如果票数大于一般则选举成功
	switch result {
	case ELECTION_SUCCESS:
		rf.setStatus(LEADER)
		MyDebug(dVote, "S%d win election", rf.me)
		MyDebug(dStatus, "S%d -> LEADER", rf.me)
		// 对所有服务器发送日志追加消息
		rf.nextIndex = make([]int, len(rf.peers))
		lastestIdx := len(rf.Logs)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = lastestIdx + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.appendEntriesLoop()
	case ELECTION_FAILED:
		MyDebug(dVote, "S%d failed election", rf.me)
	case RECONVERT_FOLLOWER:
		MyDebug(dVote, "S%d cannot join election - status is FOLLOWER", rf.me)
	case ELECTION_TIMEOUT:
		MyDebug(dVote, "S%d election timeout", rf.me)
	case KILLED_ABORT:
		MyDebug(dVote, "S%d been killed", rf.me)
	}
}

func (rf *Raft) sendAllElectionRequests(voteSuccessChan chan int, voteResultChan chan int) {
	var wg sync.WaitGroup
	voteSuccessNum := 1
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.Logs) == 0 {
		lastLogIndex = 0
		lastLogTerm = 0
	} else {
		lastLogIndex = len(rf.Logs)
		lastLogTerm = rf.Logs[lastLogIndex-1].Term
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		wg.Add(1)

		requestVoteArgs := RequestVoteArgs{
			Term:         rf.getCurrentTerm(),
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		requestVoteReply := RequestVoteReply{}

		go func(server int) {
			defer wg.Done()

			if rf.killed() {
				select {
				case voteResultChan <- KILLED_ABORT:
				default:
				}
				return
			}

			// TODO 在投票之前应该判断一下自己到底还需不需要投票
			// currentStatus := rf.getStatus()
			// if currentStatus == LEADER {

			// }

			ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			if !ok {
				MyDebug(dVote, "S%d can't connect S%d", rf.me, server)
				return
			}

			if rf.getStatus() == FOLLOWER { // 状态已经变为FOLLOWER，不再有选举资格
				select {
				case voteResultChan <- RECONVERT_FOLLOWER:
				default:
				}
			} else if requestVoteReply.Term > rf.getCurrentTerm() { // 响应中的term大于当前的term，状态变为FOLLOWER，取消选举资格
				select {
				case voteResultChan <- RECONVERT_FOLLOWER:
				default:
				}

				rf.setStatus(FOLLOWER)
				rf.setCurrentTerm(requestVoteReply.Term)
				rf.persist()
				MyDebug(dVote, "S%d exit election - currentTerm(%d) < replyTerm(%d)", rf.me, rf.getCurrentTerm(), requestVoteReply.Term)
				MyDebug(dStatus, "S%d -> FOLLOWER", rf.me)
			} else {
				if requestVoteReply.VoteGranted {
					rf.mu.Lock()
					voteSuccessNum++
					rf.mu.Unlock()
					voteSuccessChan <- 1
					MyDebug(dVote, "S%d <- S%d got vote", rf.me, server)
				} else {
					MyDebug(dVote, "S%d <- S%d refuse vote", rf.me, server)
				}
			}
		}(i)
	}
	wg.Wait()
	close(voteSuccessChan)
	if voteSuccessNum <= len(rf.peers)/2 {
		select {
		case voteResultChan <- ELECTION_FAILED:
		default:
		}
	}
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

			go func(server int) {

				var entries []Log
				var prevLogIndex int
				var prevLogTerm int

				prevLogIndex = rf.nextIndex[server] - 1
				if prevLogIndex < 1 {
					prevLogTerm = -1
				} else {
					prevLogTerm = rf.Logs[prevLogIndex-1].Term
				}

				for i := prevLogIndex + 1; i <= len(rf.Logs); i++ {
					entries = append(entries, rf.Logs[i-1])
				}

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

				rf.mu.Lock()
				if reply.Success {

					// 心跳包
					if len(entries) == 0 {
						rf.mu.Unlock()
						return
					}

					// 计算出应该提交的日志
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
					if rf.commitIndex > rf.lastApplied && rf.Logs[rf.commitIndex-1].Term == rf.CurrentTerm {
						for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
							applyMsg := ApplyMsg{
								CommandValid: true,
								Command:      rf.Logs[i-1].Command,
								CommandIndex: i,
							}
							rf.applyCh <- applyMsg
							MyDebug(dCommit, "S%d -> Client MSG:%v", rf.me, applyMsg)
						}
						rf.lastApplied = rf.commitIndex
					}
				} else {
					beforeNextIndex := rf.nextIndex[server]
					// optimized to reduce the number of rejected AppendEntries RPCs.
					if reply.Conflict {
						if reply.ConflictTermFirstIndex == 0 {
							rf.nextIndex[server] = 1
						} else {
							if rf.Logs[reply.ConflictTermFirstIndex-1].Term == reply.ConflictTerm {
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
				}
				rf.mu.Unlock()
			}(i)
		}

		time.Sleep(HAERTSBEAT_INTERVAL)
	}
}

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
		MyDebug(dLog, "S%d <- S%d accept append log")
		MyDebug(dStatus, "S%d -> FOLLOWER", rf.me)
		rf.status = FOLLOWER
	}

	if args.PrevLogIndex > len(rf.Logs) {
		var conflictTerm int
		var conflictTermFirstIndex int
		if len(rf.Logs) == 0 {
			conflictTerm = -1
			conflictTermFirstIndex = 0
		} else {
			conflictTerm = rf.Logs[len(rf.Logs)-1].Term
			for i := 1; i <= len(rf.Logs); i++ {
				if rf.Logs[i-1].Term == reply.ConflictTerm {
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
			rf.me, args.LeaderId, args.PrevLogIndex, len(rf.Logs))
		return
	}

	// an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if args.PrevLogIndex-1 >= 0 && rf.Logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.Conflict = true
		reply.ConflictTerm = rf.Logs[args.PrevLogIndex-1].Term
		for i := 1; i <= len(rf.Logs); i++ {
			if rf.Logs[i-1].Term == reply.ConflictTerm {
				reply.ConflictTermFirstIndex = i
				break
			}
		}
		rf.Logs = rf.Logs[:args.PrevLogIndex-1]
		reply.Success = false
		reply.Term = rf.CurrentTerm
		MyDebug(dLog, "S%d -> S%d refuse log append - log conflict", rf.me, args.LeaderId)
		return
	}

	// 空entries的心跳包
	if len(args.Entries) == 0 {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			originCommitIndex := rf.commitIndex
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.Logs))))
			MyDebug(dCommit, "S%d commidIndex %d -> %d LOGS:%v", rf.me, originCommitIndex, rf.commitIndex, rf.Logs)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Logs[i-1].Command,
					CommandIndex: i,
				}
				rf.applyCh <- applyMsg
				MyDebug(dCommit, "S%d -> Client MSG:%v", rf.me, applyMsg)
			}
			rf.lastApplied = rf.commitIndex
		}
		reply.Success = true
		reply.Term = rf.CurrentTerm
		return
	}

	// Append any new entries not already in the log
	rf.Logs = rf.Logs[:args.PrevLogIndex]
	rf.Logs = append(rf.Logs, args.Entries...)
	reply.Success = true
	reply.Term = rf.CurrentTerm
	MyDebug(dLog, "S%d <- S%d accept log append LOG:%v", rf.me, args.LeaderId, args.Entries)

}

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
