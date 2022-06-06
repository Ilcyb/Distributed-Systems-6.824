package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func randServerNo(prevServerNo int, servers []*labrpc.ClientEnd) int {
	newRandomServerNo := int(nrand() % int64(len(servers)))
	for newRandomServerNo == prevServerNo {
		newRandomServerNo = int(nrand() % int64(len(servers)))
	}
	return newRandomServerNo
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	getArgs := GetArgs{
		Key:      key,
		SerialNo: nrand(),
	}

	getReply := GetReply{}

	if ck.leader == -1 {
		return ck.getDetermineLeader(&getArgs, &getReply)
	} else {
		DPrintf(dClark, "Clark -> Client%d - request GET(Key:%s)", ck.leader, key)
		ok := ck.servers[ck.leader].Call("KVServer.Get", &getArgs, &getReply)
		if !ok {
			DPrintf(dClark, "Clark cannot connect old leader Client%d", ck.leader)
		} else if getReply.Err != OK {
			DPrintf(dClark, "Clark <- Client%d - request GET(Key:%s) received error message:%s", ck.leader, key, getReply.Err)
		}
		if ok && getReply.Err == OK {
			DPrintf(dClark, "Clark <- Client%d - request GET(Key:%s) success and get the VALUE:%s", ck.leader, getArgs.Key, getReply.Value)
			return getReply.Value
		}
		return ck.getDetermineLeader(&getArgs, &getReply)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putAppendArgs := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SerialNo: nrand(),
	}
	putAppendReply := PutAppendReply{}
	if ck.leader == -1 {
		ck.putDetermineLeader(&putAppendArgs, &putAppendReply)
	} else {
		DPrintf(dClark, "Clark -> Client%d - request %s(Key:%s, Value:%s, SerialNo:%d)",
			ck.leader, op, key, value, putAppendArgs.SerialNo)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		if !ok {
			DPrintf(dClark, "Clark cannot connect old leader Client%d", ck.leader)
		} else if putAppendReply.Err != OK {
			DPrintf(dClark, "Clark <- Client%d - request %s(Key:%s, Value:%s, SerialNo:%d) received error message:%s",
				ck.leader, op, key, value, putAppendArgs.SerialNo, putAppendReply.Err)
		}
		if ok && putAppendReply.Err == OK {
			DPrintf(dClark, "Clark <- Client%d - request %s(Key:%s, Value:%s, SerialNo:%d) success",
				ck.leader, op, key, value, putAppendArgs.SerialNo)
			return
		}
		ck.putDetermineLeader(&putAppendArgs, &putAppendReply)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getDetermineLeader(getArgs *GetArgs, getReply *GetReply) string {
	var serverNo int
	for {
		serverNo = randServerNo(serverNo, ck.servers)
		DPrintf(dClark, "Clark -> Client%d - request GET(Key:%s)", serverNo, getArgs.Key)
		getReply.Err = OK
		ok := ck.servers[serverNo].Call("KVServer.Get", getArgs, getReply)
		if !ok {
			DPrintf(dClark, "Clark cannot connect Client%d", serverNo)
			continue
		}
		if getReply.Err == OK {
			ck.leader = serverNo
			DPrintf(dClark, "Clark <- Client%d - request GET(Key:%s) success and get the VALUE:%s", serverNo, getArgs.Key, getReply.Value)
			return getReply.Value
		} else {
			DPrintf(dClark, "Clark <- Client%d - request GET(Key:%s) received error message:%s", serverNo, getArgs.Key, getReply.Err)
		}
	}
}

func (ck *Clerk) putDetermineLeader(putArgs *PutAppendArgs, putReply *PutAppendReply) {
	var serverNo int
	for {
		serverNo = randServerNo(serverNo, ck.servers)
		DPrintf(dClark, "Clark -> Client%d - request %s(Key:%s, Value:%s, SerialNo:%d)",
			serverNo, putArgs.Op, putArgs.Key, putArgs.Value, putArgs.SerialNo)
		putReply.Err = OK
		ok := ck.servers[serverNo].Call("KVServer.PutAppend", putArgs, putReply)
		if !ok {
			DPrintf(dClark, "Clark cannot connect Client%d", serverNo)
			continue
		}
		if putReply.Err == OK {
			ck.leader = serverNo
			DPrintf(dClark, "Clark <- Client%d - request %s(Key:%s, Value:%s, SerialNo:%d) success",
				serverNo, putArgs.Op, putArgs.Key, putArgs.Value, putArgs.SerialNo)
			return
		} else {
			DPrintf(dClark, "Clark <- Client%d - request %s(Key:%s, Value:%s, SerialNo:%d) received error message:%s",
				serverNo, putArgs.Op, putArgs.Key, putArgs.Value, putArgs.SerialNo, putReply.Err)
		}
	}
}
