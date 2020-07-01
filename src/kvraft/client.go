package raftkv

import (
	"crypto/rand"
	"cs408/labrpc"
	"math/big"
	"strconv"
	"strings"
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
	var cmd strings.Builder
	cmd.WriteString("Get")
	cmd.WriteString(OPSeparator)
	cmd.WriteString(key)
	getArgs := &GetArgs{
		Op: Op{
			ContextId: strconv.FormatInt(nrand(), 36) + CONTEXTSEPARATOR + strconv.FormatInt(nrand(), 12),
			Cmd:       cmd.String(),
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
					if reply.Err == ErrNoKey {
						// log error
						return reply.Value
					}
					if !ok || reply.WrongLeader {
						continue
					}
					ck.lastLeader = i
					if reply.Err == OK {
						DPrintf("Success get: contextID: %s", getArgs.Op.ContextId)
						return reply.Value
					}
				}
			} else {
				reply := &GetReply{}
				ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", getArgs, reply)
				if reply.Err == ErrNoKey {
					// log error
					return reply.Value
				}
				if !ok || reply.WrongLeader {
					ck.lastLeader = -1
				} else if reply.Err == OK {
					DPrintf("Success get: contextID: %s", getArgs.Op.ContextId)
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

	var cmd strings.Builder
	cmd.WriteString(op)
	cmd.WriteString(OPSeparator)
	cmd.WriteString(key)
	cmd.WriteString(KeyValueSeparator)
	cmd.WriteString(value)
	args := &PutAppendArgs{
		Op: Op{
			ContextId: strconv.FormatInt(nrand(), 36) + CONTEXTSEPARATOR + strconv.FormatInt(nrand(), 36),
			Cmd:       cmd.String(),
		},
	}
	// var prevLdr int = ck.lastLeader
	// DPrintf("begin args: %v", args)
	for {
		if ck.lastLeader < 0 {
			for i := range ck.servers {
				reply := &PutAppendReply{}
				ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
				if !ok || reply.WrongLeader {
					// DPrintf("noleader %v, %v, %v", ok, reply.WrongLeader, args)
					continue
				}
				ck.lastLeader = i
				if args.Tries > 0 {
					if reply.Err == OK {
						DPrintf("Success append: contextID: %s", args.Op.ContextId)
						return
					}
					args.Tries = 0
				} else if reply.Err == OK {
					args.Tries++
				}
				break
			}
		} else {
			reply := &PutAppendReply{}
			ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply)
			if !ok || reply.WrongLeader {
				// DPrintf("noleader2  %v, %v, %v", ok, reply.WrongLeader, args)
				// prevLdr = ck.lastLeader
				ck.lastLeader = -1
			} else if args.Tries > 0 {
				if reply.Err == OK {
					DPrintf("Success append: contextID: %s", args.Op.ContextId)
					return
				}
				args.Tries = 0
			} else if reply.Err == OK {
				args.Tries++
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
