package raftkv

const (
	WAIT     = "WAIT"
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    Op
	OK    bool
	Tries int
	Idx   int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Idx         int
}

type GetArgs struct {
	Key string
	Op  Op
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
