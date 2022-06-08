package kvraft

import (
	"bytes"
	"fmt"
	"time"

	"6.824/labgob"
	"6.824/raft"
)

func (kv *KVServer) getEncodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.CommandTable)
	e.Encode(kv.ChanTable)
	e.Encode(kv.KvMap)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *KVServer) readSnapshotData(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var commandTable map[int64]bool
	var chanTable map[int64]chan raft.ApplyMsg
	var kvMap map[string]string
	if d.Decode(&commandTable) != nil ||
		d.Decode(&chanTable) != nil ||
		d.Decode(&kvMap) != nil {
		fmt.Printf("KV服务器#%d 读取snapshot数据失败", kv.me)
	} else {
		kv.mu.Lock()
		kv.CommandTable = commandTable
		kv.ChanTable = chanTable
		kv.KvMap = kvMap
		kv.mu.Unlock()
	}
}

func (kv *KVServer) snapshotLoop(persister *raft.Persister) {
	for {
		time.Sleep(1000 * time.Millisecond)

		if kv.killed() || kv.maxraftstate == -1 {
			continue
		}

		if persister.RaftStateSize() >= kv.maxraftstate {
			kv.rf.Snapshot(kv.latestAppliedRaftLogIndex, kv.getEncodeState())
		}
	}
}
