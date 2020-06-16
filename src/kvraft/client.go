package raftkv

import (
	"crypto/rand"
	"cs408/labrpc"
	"math/big"
	"strconv"
)

var CONTEXTSEPARATOR = ":"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	getArgs := &GetArgs{
		Key: key,
		Op: Op{
			ContextId: strconv.FormatInt(nrand(), 10) + CONTEXTSEPARATOR + strconv.FormatInt(nrand(), 10),
		},
	}

	// If there is a lastleader, we will try that first,
	// if there are no last leader, we need to try all
	// servers and set the success one as lastleader
	return func() string {
		for {
			if ck.lastLeader < 0 {
				for i, s := range ck.servers {
					reply := &GetReply{}
					ok := s.Call("RaftKV.Get", getArgs, reply)
					if !ok || reply.WrongLeader {
						continue
					}
					ck.lastLeader = i
					return reply.Value
				}
			} else {
				reply := &GetReply{}
				ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", getArgs, reply)
				if !ok || reply.WrongLeader {
					ck.lastLeader = -1
				} else {
					return reply.Value
				}
			}
		}
	}()
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:    key,
		Value:  value,
		OpType: op,
		Op: Op{
			ContextId: strconv.FormatInt(nrand(), 10) + CONTEXTSEPARATOR + strconv.FormatInt(nrand(), 10),
		},
	}

	for {
		if ck.lastLeader < 0 {
			for i := range ck.servers {
				reply := &PutAppendReply{}
				ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
				if !ok || reply.WrongLeader {
					// DPrintf("noleader %v, %v", ok, reply.WrongLeader)
					continue
				}
				ck.lastLeader = i
				// DPrintf("Here")
				return
			}
		} else {
			reply := &PutAppendReply{}
			ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply)
			if !ok || reply.WrongLeader {
				// DPrintf("noleader2  %v, %v", ok, reply.WrongLeader)
				ck.lastLeader = -1
			} else {
				// DPrintf("Here")
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
