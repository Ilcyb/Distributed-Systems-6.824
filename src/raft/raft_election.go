package raft

import (
	"sync"
	"time"
)

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

	MyDebug(dVote, "S%d received vote request from S%d VOTE:{Term:%d, LLogIndex:%d, LLogTerm:%d}",
		rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		MyDebug(dVote, "S%d refuse vote to S%d - requestTerm(%d) < currentTerm(%d) ", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.status = FOLLOWER
		// rf.heartsbeat = true
		MyDebug(dStatus, "S%d -> FOLLOWER - received vote request term(%d) > currentTerm(%d)", rf.me, args.Term, rf.CurrentTerm)
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		reply.Term = rf.CurrentTerm
	}

	if rf.status == LEADER {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		MyDebug(dVote, "S%d refuse vote to S%d - is LEADER", rf.me, args.CandidateId)
		return
	}

	//  the RPC includes information about the candidate’s log, and the
	// voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	lastLogIndex := rf.getLogLen()
	lastLog := rf.getLog(lastLogIndex)
	if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		MyDebug(dVote, "S%d refuse vote to S%d - request lastLogIndex(%d) < lastLogIndex(%d) ",
			rf.me, args.CandidateId, args.LastLogIndex, lastLogIndex)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && args.Term == rf.CurrentTerm && args.LastLogIndex >= lastLogIndex {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.heartsbeat = true
		MyDebug(dVote, "S%d vote to S%d", rf.me, args.CandidateId)
		return
	}

	MyDebug(dVote, "S%d refuse vote to S%d - not satisified voteFor:S%d request.LLogIndxe(%d) self.LLogIndex(%d) self.cmtIndex(%d)",
		rf.me, args.CandidateId, rf.VoteFor, args.LastLogIndex, lastLogIndex, rf.commitIndex)
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

func (rf *Raft) election() {
	if rf.getStatus() == LEADER {
		MyDebug(dVote, "S%d is already LEADER, not allowd election", rf.me)
		return
	}
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
		rf.mu.Lock()
		rf.status = LEADER
		MyDebug(dVote, "S%d win election", rf.me)
		MyDebug(dStatus, "S%d -> LEADER", rf.me)
		// 对所有服务器发送日志追加消息
		rf.nextIndex = make([]int, len(rf.peers))
		// lastestIdx := len(rf.Logs)
		lastestIdx := rf.getLogLen()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = lastestIdx + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		// rf.matchIndex[rf.me] = lastestIdx
		go rf.appendEntriesLoop()
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
	// if len(rf.Logs) == 0 {
	if rf.getLogLen() == 0 {
		lastLogIndex = 0
		lastLogTerm = -1
	} else {
		// lastLogIndex = len(rf.Logs)
		lastLogIndex = rf.getLogLen()
		// lastLogTerm = rf.Logs[lastLogIndex-1].Term
		lastLogTerm = rf.getLog(lastLogIndex).Term
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

				MyDebug(dVote, "S%d exit election - currentTerm(%d) < replyTerm(%d)", rf.me, rf.getCurrentTerm(), requestVoteReply.Term)
				rf.setStatus(FOLLOWER)
				rf.setCurrentTerm(requestVoteReply.Term)
				rf.persist()
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
