package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operate  int
	Object   string
	Value    string
	SerialNo uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandTable map[uint64]bool
	chanTable    map[uint64]chan raft.ApplyMsg
	serialNo     uint64
	kvMap        map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Object:   args.Key,
		SerialNo: atomic.AddUint64(&kv.serialNo, 1),
		Operate:  GET,
	}

	_, err := kv.startRaft(command)
	if err != OK {
		reply.Err = Err(err)
		return
	}

	DPrintf("S%d executed command(Op:%s, Key:%s, SerialNo:%d)",
		kv.me, opNameMap[command.Operate], command.Object, command.SerialNo)

	kv.mu.Lock()
	reply.Value = kv.kvMap[args.Key]
	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	command := Op{
		Object: args.Key,
		Value:  args.Value,
	}
	if args.SerialNo == 0 {
		command.SerialNo = atomic.AddUint64(&kv.serialNo, 1)
	} else {
		command.SerialNo = args.SerialNo
	}
	reply.SerialNo = command.SerialNo

	if args.Op == "Put" {
		command.Operate = PUT
	} else if args.Op == "Append" {
		command.Operate = APPEND
	} else {
		panic("should not be here")
	}

	applyMsg, err := kv.startRaft(command)
	if err != OK {
		reply.Err = Err(err)
		return
	}

	applyCommand := applyMsg.Command.(Op)
	kv.mu.Lock()
	kv.executeCommand(applyCommand)
	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *KVServer) startRaft(command Op) (raft.ApplyMsg, string) {
	err := OK

	index, term, accept := kv.rf.Start(command)
	if !accept {
		err = ErrWrongLeader
		return raft.ApplyMsg{}, err
	}

	applyCh := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	kv.commandTable[command.SerialNo] = false
	kv.chanTable[command.SerialNo] = applyCh
	kv.mu.Unlock()

	DPrintf("S%d started command(Op:%s, Key:%s, Value:%s, Idx:%d, Term:%d, SeriNo:%d)",
		kv.me, opNameMap[command.Operate], command.Object, command.Value, index, term, command.SerialNo)

	var applyMsg raft.ApplyMsg
	select {
	case applyMsg = <-applyCh:
	case <-time.After(CONSENSUS_TIMEOUT * time.Millisecond):
		DPrintf("S%d execute command(Op:%s, Key:%s, Value:%s, Idx:%d, Term:%d) timeout",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, index, term)
		if !kv.rf.IsLeader() {
			err = ErrWrongLeader
		} else {
			err = ErrTimeout
		}
	}

	kv.mu.Lock()
	delete(kv.chanTable, command.SerialNo)
	kv.mu.Unlock()

	return applyMsg, err
}

func (kv *KVServer) applyLoop() {
	for {
		if kv.killed() {
			continue
		}

		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.applyCommandMsg(applyMsg)
		} else if applyMsg.SnapshotValid {
			kv.applySnapshotMsg(applyMsg)
		}

	}
}

func (kv *KVServer) applyCommandMsg(applyMsg raft.ApplyMsg) {
	applyCommand := applyMsg.Command.(Op)
	kv.mu.Lock()

	// commandTable中存在该serialNo，说明该Server需要向Client返回响应
	if executed, exists := kv.commandTable[applyCommand.SerialNo]; exists {
		// 丢弃已经执行过的命令
		if executed {
			DPrintf("S%d Drop repeated command applyMsg(command:%v)", kv.me, applyCommand)
			kv.mu.Unlock()
			return
		}

		if _, chanExists := kv.chanTable[applyCommand.SerialNo]; chanExists {
			kv.chanTable[applyCommand.SerialNo] <- applyMsg
		} else {
			DPrintf("S%d kv applyChain[%d] is not exists, a timeout command has been executed", kv.me, applyCommand.SerialNo)
			kv.executeCommand(applyCommand)
		}

		// 是其他Raft server start 的命令，因此在自己的commandTable中没有记录，只需要执行而不需要响应
	} else {
		kv.executeCommand(applyCommand)
	}

	kv.commandTable[applyCommand.SerialNo] = true

	kv.mu.Unlock()
}

func (kv *KVServer) applySnapshotMsg(applyMsg raft.ApplyMsg) {
}

func (kv *KVServer) commandTableGCLoop() {
	for {
		time.Sleep(time.Second)
		if kv.killed() {
			continue
		}

		currentSerialNo := atomic.LoadUint64(&kv.serialNo)

		kv.mu.Lock()
		for serialNo := range kv.commandTable {
			if currentSerialNo-serialNo > 1000 {
				delete(kv.commandTable, serialNo)
			}
		}
		kv.mu.Unlock()

		// for serialNo := range kv.chanTable {
		// 	if currentSerialNo-serialNo > 100 {
		// 		kv.mu.Lock()
		// 		kv.chanTable[serialNo] <- raft.ApplyMsg{CommandValid: false, SnapshotValid: false}
		// 		kv.mu.Unlock()
		// 	}
		// }
	}
}

func (kv *KVServer) executeCommand(command Op) {
	if command.Operate == PUT {
		kv.kvMap[command.Object] = command.Value
		DPrintf("S%d executed command(Op:%s, Key:%s, Value:%s, SerialNo:%d)",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, command.SerialNo)
	} else if command.Operate == APPEND {
		kv.kvMap[command.Object] += command.Value
		DPrintf("S%d executed command(Op:%s, Key:%s, Value:%s, SerialNo:%d)",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, command.SerialNo)
	} else if command.Operate == GET {
		DPrintf("S%d received command(Op:%s, Key:%s, SerialNo:%d) and ignored",
			kv.me, opNameMap[command.Operate], command.Object, command.SerialNo)
	} else {
		panic("should not be here")
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.commandTable = make(map[uint64]bool)
	kv.chanTable = make(map[uint64]chan raft.ApplyMsg)
	kv.serialNo = 1
	kv.kvMap = make(map[string]string)

	go kv.applyLoop()
	go kv.commandTableGCLoop()

	return kv
}
