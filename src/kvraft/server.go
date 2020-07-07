package raftkv

import (
	"cs408/labrpc"
	"cs408/raft"
	"encoding/gob"
	"fmt"
	"log"
	"strings"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var OPSeparator string = ":::"
var KeyValueSeparator string = "<<>>"
var APPEND = "Append"
var PUT = "Put"
var GET = "Get"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ContextId string
	Cmd       string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	errCh   chan string

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// commited raft Log
	log map[int]string

	// Fake database (state machine)
	db map[string]string

	// Read ops chan
	ops map[int]chan struct {
		Val string
		Ok  bool
	}

	context map[string]int
}

//
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}
	// _, ok := kv.context[args.Op.ContextId]
	// if !ok {
	// 	kv.context[args.Op.ContextId] = true
	// } else {
	// 	return
	// }
	idx, _, _ := kv.rf.Start(args.Op)
	kv.context[args.Op.ContextId] = idx
	_, ok := kv.ops[idx]
	if !ok {
		kv.mu.Lock()
		kv.ops[idx] = make(chan struct {
			Val string
			Ok  bool
		}, 1)
		kv.mu.Unlock()
	}
	DPrintf("get waiting %s", args.Op.ContextId)
	// TODO: A timeout
	var re struct {
		Val string
		Ok  bool
	}

	select {
	case re = <-kv.ops[idx]:
		kv.ops[idx] <- re
	}
	DPrintf("get done %s", args.Op.ContextId)
	key, val := splitPutAppendCmd(re.Val)
	// delete(kv.ops, idx)
	if key == args.Op.Cmd {
		reply.Value = val
		reply.Err = OK
	} else if key == args.Op.Cmd {
		reply.Err = ErrNoKey
	}
}

//
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	if !isLeader {
		return
	}
	var idx int = args.Idx
	var ok bool
	var re struct {
		Val string
		Ok  bool
	}

	DPrintf("Context: args: %v", args)

	// DPrintf("%v leader", isLeader)
	if args.Idx == 0 {
		idx, ok := kv.context[args.Op.ContextId]
		if !ok {
			idx, _, _ = kv.rf.Start(args.Op)
			kv.context[args.Op.ContextId] = idx
		}
		reply.Err = OK
		reply.Idx = idx
		return
	}
	_, ok = kv.ops[idx]
	if !ok {
		kv.mu.Lock()
		kv.ops[idx] = make(chan struct {
			Val string
			Ok  bool
		}, 1)
		kv.mu.Unlock()
	}
	// DPrintf("append waiting %s", args.Op.ContextId)
	DPrintf("Waiting on ctx: %s", args.Op.ContextId)
	select {
	case re = <-kv.ops[idx]:
		kv.ops[idx] <- re
		// delete(kv.ops, idx)
		if re.Ok && re.Val == args.Op.Cmd {
			reply.Err = OK
			DPrintf("Correct Value %v, at %s", re.Val, args.Op.ContextId)
		} else {
			reply.Err = ErrNoKey
			DPrintf("Wrong Value %v, at %s", re.Val, args.Op.ContextId)
		}
	}

}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.ops = make(map[int]chan struct {
		Val string
		Ok  bool
	})
	kv.db = make(map[string]string)
	kv.log = make(map[int]string)
	kv.errCh = make(chan string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.context = make(map[string]int)

	go func() {
		for errmsg := range kv.errCh {
			fmt.Println(errmsg)
		}
	}()

	// Read applych
	go func() {
		for msg := range kv.applyCh {
			if m, ok := msg.Command.(Op); ok {
				old, ok := kv.log[msg.Index]
				if !ok {
					// TODO: Check if prev index (currIndex - 1) exists
					kv.log[msg.Index] = m.Cmd

					// apply to state machine (for put / append)
					op, val := parseCmd(m.Cmd)
					DPrintf("%d: ApplyCh: %v", me, msg)
					kv.mu.Lock()
					kv.context[m.ContextId] = msg.Index
					kv.mu.Unlock()
					switch op {
					case PUT:
						k, v := splitPutAppendCmd(val)
						kv.mu.Lock()
						// fmt.Printf("put %v\n", v)
						kv.db[k] = v
						if _, ok := kv.ops[msg.Index]; !ok {
							kv.ops[msg.Index] = make(chan struct {
								Val string
								Ok  bool
							}, 1)
						}
						kv.mu.Unlock()
						select {
						case kv.ops[msg.Index] <- struct {
							Val string
							Ok  bool
						}{m.Cmd, true}:
						default:
						}

					case APPEND:
						k, v := splitPutAppendCmd(val)
						if _, ok := kv.db[k]; ok {
							// fmt.Printf("append %v\n", v)
							kv.mu.Lock()
							kv.db[k] += v
							if _, ok := kv.ops[msg.Index]; !ok {
								kv.ops[msg.Index] = make(chan struct {
									Val string
									Ok  bool
								}, 1)
							}
							kv.mu.Unlock()
							select {
							case kv.ops[msg.Index] <- struct {
								Val string
								Ok  bool
							}{m.Cmd, true}:
							default:
							}
						}
					case GET:
						kv.mu.Lock()
						if _, ok := kv.ops[msg.Index]; !ok {
							kv.ops[msg.Index] = make(chan struct {
								Val string
								Ok  bool
							}, 1)
						}
						kv.mu.Unlock()
						if getVal, ok := kv.db[val]; ok {
							select {
							case kv.ops[msg.Index] <- struct {
								Val string
								Ok  bool
							}{m.Cmd + KeyValueSeparator + getVal, true}:
							default:
							}

						} else {
							select {
							case kv.ops[msg.Index] <- struct {
								Val string
								Ok  bool
							}{"", false}:
							default:
							}
						}
					}

				} else {
					kv.errCh <- fmt.Sprintf("%d: msg:%v already in idx:%d, old:%v", kv.me, m, msg.Index, old)
					select {
					case kv.ops[msg.Index] <- struct {
						Val string
						Ok  bool
					}{old, true}:
					default:
					}

				}
			} else {
				kv.errCh <- fmt.Sprintf("%d: msg:%v, idx:%d is not a string", kv.me, m, msg.Index)
			}
		}
	}()

	return kv
}

func parseCmd(cmd string) (key string, val string) {
	s := strings.Split(cmd, OPSeparator)
	if len(s) < 2 {
		return
	}

	return s[0], s[1]
}

func splitPutAppendCmd(putappend string) (key string, val string) {
	s := strings.Split(putappend, KeyValueSeparator)
	if len(s) < 2 {
		return
	}
	key, val = s[0], s[1]
	return
}
