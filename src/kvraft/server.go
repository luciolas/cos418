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

const Debug = 1

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
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	errCh   chan string

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// Raft Log
	log map[int]string

	// Fake database
	db map[string]string

	// Read ops chan
	ops map[int]chan struct {
		Val string
		Ok  bool
	}

	context map[string]bool
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
	var cmd strings.Builder
	cmd.WriteString("Get")
	cmd.WriteString(OPSeparator)
	cmd.WriteString(args.Key)
	idx, _, _ := kv.rf.Start(cmd.String())
	kv.ops[idx] = make(chan struct {
		Val string
		Ok  bool
	})

	// TODO: A timeout
	re := <-kv.ops[idx]
	delete(kv.ops, idx)
	if re.Ok {
		reply.Value = re.Val
		reply.Err = OK
	} else {
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
	_, ok := kv.context[args.Op.ContextId]
	if !ok {
		kv.context[args.Op.ContextId] = true
	} else {
		return
	}

	var cmd strings.Builder
	cmd.WriteString(args.OpType)
	cmd.WriteString(OPSeparator)
	cmd.WriteString(args.Key)
	cmd.WriteString(KeyValueSeparator)
	cmd.WriteString(args.Value)
	idx, _, _ := kv.rf.Start(cmd.String())
	// DPrintf("%v leader", isLeader)
	kv.ops[idx] = make(chan struct {
		Val string
		Ok  bool
	})

	re := <-kv.ops[idx]
	if re.Ok {
		reply.Err = OK
		return
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
	kv.context = make(map[string]bool)

	go func() {
		for errmsg := range kv.errCh {
			fmt.Println(errmsg)
		}
	}()

	// Read applych
	go func() {
		for msg := range kv.applyCh {
			if m, ok := msg.Command.(string); ok {
				old, ok := kv.log[msg.Index]
				if !ok {
					// TODO: Check if prev index (currIndex - 1) exists
					kv.log[msg.Index] = m

					// apply to state machine (for put / append)

					op, val := parseCmd(m)
					kv.mu.Lock()
					switch op {
					case PUT:
						k, v := splitPutAppendCmd(val)
						// fmt.Printf("put %v\n", v)
						kv.db[k] = v
						opChnl, ok := kv.ops[msg.Index]
						if ok {
							opChnl <- struct {
								Val string
								Ok  bool
							}{"", ok}
						}
					case APPEND:
						k, v := splitPutAppendCmd(val)
						if _, ok := kv.db[k]; ok {
							// fmt.Printf("append %v\n", v)
							kv.db[k] += v
							opChnl, ok := kv.ops[msg.Index]
							if ok {
								opChnl <- struct {
									Val string
									Ok  bool
								}{"", ok}
							}
						}
					case GET:
						readOpChnl, ok := kv.ops[msg.Index]
						if ok {
							if getVal, ok := kv.db[val]; ok {
								readOpChnl <- struct {
									Val string
									Ok  bool
								}{getVal, ok}
							} else {
								readOpChnl <- struct {
									Val string
									Ok  bool
								}{"", ok}
							}
						}

					}

					kv.mu.Unlock()

				} else {
					kv.errCh <- fmt.Sprintf("%d: msg:%v already in idx:%d, old:%v", kv.me, m, msg.Index, old)
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
