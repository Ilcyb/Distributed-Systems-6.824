package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

const CONSENSUS_TIMEOUT = 500

var opNameMap = map[int]string{
	GET:    "GET",
	PUT:    "PUT",
	APPEND: "APPEND",
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialNo uint64
}

type PutAppendReply struct {
	Err      Err
	SerialNo uint64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
