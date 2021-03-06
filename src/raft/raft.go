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

	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	currentTerm int               // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     *labrpc.ClientEnd // candidateId that received vote in current term (or null if none)
	logs        []Log             // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders and all of it reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// ????????????leader????????????
	heartsbeat bool // ???election timeout?????????????????????????????????
	status     int  // ?????????????????????
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
	Term         int // candidate???s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate???s last log entry
	LastLogTerm  int // term of candidate???s last log entry
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
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Raft?????????#%d ??????????????????????????????Term????????????Term??????????????????", rf.me)
		return
	}

	//  the RPC includes information about the candidate???s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	var lastLog Log
	var lastLogIndex int
	if len(rf.logs)-1 < 0 {
		lastLog = Log{
			Term: -1,
		}
		lastLogIndex = 0
	} else {
		lastLog = rf.logs[len(rf.logs)-1]
		lastLogIndex = len(rf.logs)
	}
	if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Raft?????????#%d ???????????????????????????????????????Log???????????????Log??????????????????", rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = rf.peers[args.CandidateId]
		DPrintf("Raft?????????#%d ?????????????????????????????????Term????????????Term??????????????????FOLLOWER???????????????", rf.me)
		return
	}

	if rf.status == LEADER {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Raft?????????#%d ???????????????????????????LEADER??????????????????????????????", rf.me)
		return
	}

	if rf.voteFor == nil && args.Term == rf.currentTerm && args.LastLogIndex >= rf.commitIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.voteFor = rf.peers[args.CandidateId]
		DPrintf("Raft?????????#%d ?????????????????????????????????Term????????????Term???????????????????????????????????????????????????????????????????????????????????????", rf.me)
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
	DPrintf("Raft?????????#%d ??????Start?????? %v", rf.me, command)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.status != LEADER {
		return -1, -1, false
	}

	DPrintf("LEADER#%d ??????????????????????????????%v", rf.me, command)
	term := rf.currentTerm
	newLog := Log{
		Term:    term,
		Command: command,
	}
	rf.logs = append(rf.logs, newLog)
	index := len(rf.logs)
	rf.matchIndex[rf.me] = len(rf.logs)

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
		rand.Seed(time.Now().UnixNano())
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(150)+150))

		// ?????????server???leader?????????????????????????????????
		if rf.getStatus() == LEADER {
			continue
		}

		if !rf.getHeartsbeat() {
			// ?????????????????????leader????????????????????????candidate
			rf.setStatus(CANDIDATE)
			DPrintf("Raft?????????#%d ?????????????????????LEADER???????????????????????????CANDIDATE\n", rf.me)
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
	rf.setVoteFor(rf.peers[rf.me])
	voteSuccessChan := make(chan int, len(rf.peers))
	resultCh := make(chan int)
	DPrintf("Raft?????????#%d ????????????\n", rf.me)
	go rf.sendAllElectionRequests(voteSuccessChan, resultCh)
	voteNum := 1
	go func() {
		for {
			// ?????????goroutine??????????????? killed
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
					// ????????????
					select {
					case resultCh <- ELECTION_SUCCESS:
					default:
					}
					goto EXIT
				}
			case <-time.After(ELECTION_FAILED_TIMEOUT): // TODO ????????????????????????????????????
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
	// ???????????????????????????????????????
	switch result {
	case ELECTION_SUCCESS:
		rf.setStatus(LEADER)
		DPrintf("Raft?????????#%d ????????????????????????LEADER", rf.me)
		// ??????????????????????????????????????????
		rf.nextIndex = make([]int, len(rf.peers))
		lastestIdx := len(rf.logs)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = lastestIdx + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.appendEntriesLoop()
	case ELECTION_FAILED:
		DPrintf("Raft?????????#%d ????????????", rf.me)
	case RECONVERT_FOLLOWER:
		DPrintf("Raft?????????#%d ?????????FOLLOWER????????????????????????", rf.me)
	case ELECTION_TIMEOUT:
		DPrintf("Raft?????????#%d ????????????", rf.me)
	case KILLED_ABORT:
		DPrintf("Raft?????????#%d KILLED", rf.me)
	}
}

func (rf *Raft) sendAllElectionRequests(voteSuccessChan chan int, voteResultChan chan int) {
	var wg sync.WaitGroup
	voteSuccessNum := 1
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.logs) == 0 {
		lastLogIndex = 0
		lastLogTerm = 0
	} else {
		lastLogIndex = len(rf.logs)
		lastLogTerm = rf.logs[lastLogIndex-1].Term
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

			ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			if !ok {
				DPrintf("Raft?????????#%d ????????????????????????", rf.me)
				return
			}

			if rf.getStatus() == FOLLOWER { // ??????????????????FOLLOWER????????????????????????
				select {
				case voteResultChan <- RECONVERT_FOLLOWER:
				default:
				}
				DPrintf("Raft?????????#%d ??????????????????FOLLOWER????????????????????????\n", rf.me)
			} else if requestVoteReply.Term > rf.getCurrentTerm() { // ????????????term???????????????term???????????????FOLLOWER?????????????????????
				select {
				case voteResultChan <- RECONVERT_FOLLOWER:
				default:
				}

				rf.setStatus(FOLLOWER)
				rf.setCurrentTerm(requestVoteReply.Term)
				DPrintf("Raft?????????#%d ??????term:%d??????????????????????????????term:%d???????????????FOLLOWER?????????????????????\n",
					rf.me, rf.getCurrentTerm(), requestVoteReply.Term)
			} else {
				if requestVoteReply.VoteGranted {
					rf.mu.Lock()
					voteSuccessNum++
					rf.mu.Unlock()
					voteSuccessChan <- 1
					DPrintf("Raft?????????#%d ??????????????????\n", rf.me)
				} else {
					DPrintf("Raft?????????#%d ??????????????????\n", rf.me)
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
			DPrintf("Raft?????????#%d ?????????LEADER??????????????????????????????????????????\n", rf.me)
			return
		}

		DPrintf("LEADER#%d ??????????????????????????????????????????%v", rf.me, rf.getLogs())

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(server int) {

				if rf.getStatus() != LEADER {
					return
				}

				var entries []Log
				var prevLogIndex int
				var prevLogTerm int

				prevLogIndex = rf.nextIndex[server] - 1
				if prevLogIndex < 1 {
					prevLogTerm = 0
				} else {
					prevLogTerm = rf.logs[prevLogIndex-1].Term
				}
				DPrintf("prevLogIndex:%d, len(rf.logs):%d", prevLogIndex, len(rf.logs))
				for i := prevLogIndex + 1; i <= len(rf.logs); i++ {
					entries = append(entries, rf.logs[i-1])
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

				if len(entries) > 0 {
					DPrintf("Raft?????????#%d ???FOLLOWER#%d????????????????????????%v\n", rf.me, server, entries)
				} else {
					DPrintf("Raft?????????#%d ???FOLLOWER#%d???????????????\n", rf.me, server)
				}

				ok := rf.sendRequestAppendEntries(server, &args, &reply)
				if !ok {
					if len(entries) > 0 {
						DPrintf("Raft?????????#%d ???FOLLOWER#%d??????????????????????????????????????????\n", rf.me, server)
					} else {
						DPrintf("Raft?????????#%d ???FOLLOWER#%d?????????????????????????????????\n", rf.me, server)
					}
					return
				}

				// follower????????????term???leader?????????????????????leader????????????follower
				if reply.Term > rf.getCurrentTerm() {
					DPrintf("Raft?????????#%d ???????????????????????????Term(%d)???????????????Term(%d)??????LEADER?????????FOLLOWER",
						rf.me, reply.Term, rf.getCurrentTerm())
					rf.setCurrentTerm(reply.Term)
					rf.setStatus(FOLLOWER)
					return
				}

				rf.mu.Lock()
				if reply.Success {

					// ?????????
					if len(entries) == 0 {
						rf.mu.Unlock()
						return
					}

					rf.nextIndex[server] = prevLogIndex + len(entries) + 1
					// TODO ????????????????????????matchIndex?????????
					rf.matchIndex[server] = prevLogIndex + len(entries)
					DPrintf("Raft?????????#%d ?????????FOLLOWER#%d????????????%v\n", rf.me, server, entries)
					copyMatch := make([]int, len(rf.matchIndex))
					copy(copyMatch, rf.matchIndex)

					sort.Ints(copyMatch)
					var mid int
					if len(copyMatch)%2 == 0 {
						mid = len(copyMatch)/2 - 1
					} else {
						mid = len(copyMatch) / 2
					}
					majorityMatch := copyMatch[mid]
					DPrintf("sorted match:%v\n", copyMatch)
					DPrintf("majority match:%d\n", majorityMatch)
					DPrintf("rf.commitIndex:%d\n", rf.commitIndex)
					DPrintf("rf.logs:%v\n", rf.logs)
					if majorityMatch > rf.commitIndex && rf.logs[majorityMatch-1].Term == rf.currentTerm {
						for i := rf.commitIndex + 1; i <= majorityMatch; i++ {
							applyMsg := ApplyMsg{
								CommandValid: true,
								Command:      rf.logs[i-1].Command,
								CommandIndex: i,
							}
							rf.applyCh <- applyMsg
							DPrintf("Raft?????????#%d ????????????????????????????????????%v", rf.me, applyMsg)
						}
						rf.commitIndex = majorityMatch
					}
				} else {
					rf.nextIndex[server]--
				}
				rf.mu.Unlock()
			}(i)
		}

		time.Sleep(HAERTSBEAT_INTERVAL)
	}
}

type RequestAppendEntriesArgs struct {
	Term         int   // leader???s term
	LeaderId     int   // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader???s commitIndex
}

type RequestAppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// code for 2A???2B
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Success = false
		reply.Term = -1
		return
	}

	DPrintf("Raft?????????#%d ????????????????????????CommitIndex???%d ?????????CommitIndex???%d", rf.me, args.LeaderCommit, rf.commitIndex)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("Raft?????????#%d ???????????????????????????Term(%d)????????????Term(%d)???????????????", rf.me, args.Term, rf.currentTerm)
		return
	}

	rf.heartsbeat = true
	rf.currentTerm = args.Term
	rf.status = FOLLOWER

	if args.PrevLogIndex > len(rf.logs) {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("Raft?????????#%d ???????????????????????????PrevLogIndex??????????????????????????????????????????", rf.me)
		return
	}

	// an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if args.PrevLogIndex-1 >= 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex-1]
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("Raft?????????#%d ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????", rf.me)
		return
	}

	// ???entries????????????
	if len(args.Entries) == 0 {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			originCommitIndex := rf.commitIndex
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.logs))))
			DPrintf("Raft?????????#%d ???commitIndex???%d?????????%d??????????????????:%v", rf.me, originCommitIndex, rf.commitIndex, rf.logs)
			for i := originCommitIndex + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i-1].Command,
					CommandIndex: i,
				}
				rf.applyCh <- applyMsg
				DPrintf("Raft?????????#%d ????????????????????????????????????%v", rf.me, applyMsg)
			}
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	// Append any new entries not already in the log
	rf.logs = rf.logs[:args.PrevLogIndex]
	rf.logs = append(rf.logs, args.Entries...)
	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("Raft?????????#%d ?????????????????????????????????????????????%v????????????????????????", rf.me, args.Entries)

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
	rf.currentTerm = currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	returnVal := rf.currentTerm
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

func (rf *Raft) setVoteFor(vote *labrpc.ClientEnd) {
	rf.mu.Lock()
	rf.voteFor = vote
	rf.mu.Unlock()
}

func (rf *Raft) getVoteFor() *labrpc.ClientEnd {
	rf.mu.Lock()
	returnVal := rf.voteFor
	rf.mu.Unlock()
	return returnVal
}

func (rf *Raft) getLogs() []Log {
	rf.mu.Lock()
	returnVal := rf.logs
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
	rf.currentTerm = 0
	rf.logs = make([]Log, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartsbeat = false
	rf.status = FOLLOWER
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
