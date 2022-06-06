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
	SerialNo int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandTable map[int64]bool
	chanTable    map[int64]chan raft.ApplyMsg
	kvMap        map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Object:   args.Key,
		SerialNo: args.SerialNo,
		Operate:  GET,
	}
	DPrintf(dKVServer, "K%d received command(Op:%s, Key:%s, SeriNo:%d)",
		kv.me, opNameMap[command.Operate], command.Object, command.SerialNo)

	_, err := kv.startRaft(command)
	if err != OK {
		reply.Err = Err(err)
		return
	}

	DPrintf(dKVServer, "K%d executed command(Op:%s, Key:%s, SerialNo:%d)",
		kv.me, opNameMap[command.Operate], command.Object, command.SerialNo)

	kv.mu.Lock()
	reply.Value = kv.kvMap[args.Key]
	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	command := Op{
		Object:   args.Key,
		Value:    args.Value,
		SerialNo: args.SerialNo,
	}

	DPrintf(dKVServer, "K%d received command(Op:%s, Key:%s, Value:%s, SeriNo:%d)",
		kv.me, opNameMap[command.Operate], command.Object, command.Value, command.SerialNo)

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
	if executed := kv.commandTable[applyCommand.SerialNo]; !executed {
		kv.executeCommand(applyCommand)
		kv.commandTable[applyCommand.SerialNo] = true
	} else {
		DPrintf(dKVServer, "K%d Drop repeated command applyMsg(command:%v)", kv.me, applyCommand)
	}
	kv.mu.Unlock()
	reply.Err = OK
}

func (kv *KVServer) startRaft(command Op) (raft.ApplyMsg, string) {
	err := OK

	index, term, accept := kv.rf.Start(command)
	if !accept {
		DPrintf(dKVServer, "K%d refused command(Op:%s, Key:%s, Value:%s, Idx:%d, Term:%d, SeriNo:%d) cause not Leader",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, index, term, command.SerialNo)
		err = ErrWrongLeader
		return raft.ApplyMsg{}, err
	}

	applyCh := make(chan raft.ApplyMsg)
	kv.mu.Lock()
	if _, exists := kv.commandTable[command.SerialNo]; !exists {
		kv.commandTable[command.SerialNo] = false
	}
	kv.chanTable[command.SerialNo] = applyCh
	kv.mu.Unlock()

	DPrintf(dKVServer, "K%d started command(Op:%s, Key:%s, Value:%s, Idx:%d, Term:%d, SeriNo:%d)",
		kv.me, opNameMap[command.Operate], command.Object, command.Value, index, term, command.SerialNo)

	var applyMsg raft.ApplyMsg
	select {
	case applyMsg = <-applyCh:
	case <-time.After(CONSENSUS_TIMEOUT * time.Millisecond):
		DPrintf(dKVServer, "K%d execute command(Op:%s, Key:%s, Value:%s, Idx:%d, Term:%d, SeriNo:%d) timeout",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, index, term, command.SerialNo)
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

		DPrintf(dKVServer, "K%d TEST TEST Wait ApplyLoop", kv.me)
		applyMsg := <-kv.applyCh
		DPrintf(dKVServer, "K%d received applied message %v", kv.me, applyMsg)
		if applyMsg.CommandValid {
			kv.applyCommandMsg(applyMsg)
		} else if applyMsg.SnapshotValid {
			kv.applySnapshotMsg(applyMsg)
		}
		DPrintf(dKVServer, "K%d TEST TEST Exit ApplyLoop", kv.me)

	}
}

func (kv *KVServer) applyCommandMsg(applyMsg raft.ApplyMsg) {
	applyCommand := applyMsg.Command.(Op)
	kv.mu.Lock()

	// commandTable中存在该serialNo，说明该Server需要向Client返回响应
	if executed, exists := kv.commandTable[applyCommand.SerialNo]; exists {

		// commandTable中存在且有管道在等待
		if applyCommandCh, chanExists := kv.chanTable[applyCommand.SerialNo]; chanExists {
			kv.mu.Unlock()
			if applyCommandCh != nil {
				select {
				case applyCommandCh <- applyMsg:
				case <-time.After(APPLY_CHAN_DELETE_TIMEOUT * time.Millisecond):
					DPrintf(dKVServer, "K%d write applyMsg to chan[%d] while chan has been deleted", kv.me, applyCommand.SerialNo)
				}
			}
			return
		} else if !executed {
			// commandTabke中存在、没管道在等待且没被执行过
			DPrintf(dKVServer, "K%d kv applyChain[%d] is not exists, a timeout command has been executed", kv.me, applyCommand.SerialNo)
			kv.executeCommand(applyCommand)
			kv.commandTable[applyCommand.SerialNo] = true
			kv.mu.Unlock()
			return
		}
	} else {
		// 是其他Raft server start 的命令，因此在自己的commandTable中没有记录，只需要执行而不需要响应
		kv.executeCommand(applyCommand)
		kv.commandTable[applyCommand.SerialNo] = true
		kv.mu.Unlock()
		return
	}

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

		// kv.mu.Lock()
		// for serialNo := range kv.commandTable {
		// 	if currentSerialNo-serialNo > 1000 {
		// 		delete(kv.commandTable, serialNo)
		// 	}
		// }
		// kv.mu.Unlock()
	}
}

func (kv *KVServer) executeCommand(command Op) {
	if command.Operate == PUT {
		kv.kvMap[command.Object] = command.Value
		DPrintf(dKVServer, "K%d executed command(Op:%s, Key:%s, Value:%s, SerialNo:%d)",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, command.SerialNo)
	} else if command.Operate == APPEND {
		kv.kvMap[command.Object] += command.Value
		DPrintf(dKVServer, "K%d executed command(Op:%s, Key:%s, Value:%s, SerialNo:%d)",
			kv.me, opNameMap[command.Operate], command.Object, command.Value, command.SerialNo)
	} else if command.Operate == GET {
		DPrintf(dKVServer, "K%d received command(Op:%s, Key:%s, SerialNo:%d) and ignored",
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
	kv.commandTable = make(map[int64]bool)
	kv.chanTable = make(map[int64]chan raft.ApplyMsg)
	kv.kvMap = make(map[string]string)

	go kv.applyLoop()
	go kv.commandTableGCLoop()

	return kv
}
