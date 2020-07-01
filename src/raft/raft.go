package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"cs408/labrpc"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_LOGS int = 1024
)

const (
	NOT_OK = iota
	OK
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type RaftLog struct {
	Index int32
	Term  int32
	Cmd   interface{}
}

type At32 struct {
	val int32
}

func (at *At32) Set(v int32) {
	atomic.StoreInt32(&at.val, v)
}

func (at *At32) Load() int32 {
	return atomic.LoadInt32(&at.val)
}

func (at *At32) Add(v int32) int32 {
	return atomic.AddInt32(&at.val, v)
}

type AtBool struct {
	val uint32
}

func (at *AtBool) Set(v bool) {
	var in uint32
	if v {
		in = 1
	}
	atomic.StoreUint32(&at.val, in)
}

func (at *AtBool) Load() bool {
	return atomic.LoadUint32(&at.val) != 0
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	muLeader  sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int32 // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyChnl           chan ApplyMsg
	jobsChnl            chan RaftLog
	rpcChnl             chan int
	heartBeatChnl       []chan struct{}
	heartBeatTimerReset chan bool
	rdyChnl             []chan AppendEntriesArgs
	// leaderBeginChnl     chan bool
	termChangeChnl chan int32
	stopChnl       chan bool
	commitIdxChnl  chan int32
	logidx         *At32
	isLeader       *AtBool
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	//persist
	currentTerm *At32
	VotedFor    int32
	logs        []RaftLog

	//volatile
	commitIdx   *At32
	lastApplied *At32

	//volatile (leaders)
	nextIndex  []*At32
	matchIndex []*At32
}

type RaftAtSnap struct {
	mu          sync.Mutex
	currentTerm *At32
	commitIdx   *At32
	lastApplied *At32
	isLeader    bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
	term := rf.currentTerm.Load()
	isleader := rf.isLeader.Load()
	return int(term), isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm.Load())
	e.Encode(rf.VotedFor)
	e.Encode(rf.logs)
	// e.Encode(rf.logidx.Load())
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var term int32
	var logidx int32
	d.Decode(&term)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.logs)
	// d.Decode(&logidx)
	rf.currentTerm.Set(term)
	logidx = int32(len(rf.logs) - 1)
	rf.logidx.Set(logidx)
	fmt.Printf("%d: restore logs %v\n", rf.me, rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int32
	ID   int32
	Log  RaftLog
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term  int32
	Voted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.rpcChnl <- OK
	// DPrintf("%d: reqvote args %v from %d", rf.me, args, args.ID)
	// Your code here.
	currentTerm := rf.currentTerm.Load()
	reply.Term = currentTerm
	if args.Term < currentTerm {
		return
	}
	rf.currentTerm.Set(args.Term)
	reply.Term = args.Term
	if args.Term > currentTerm || (currentTerm == args.Term && rf.VotedFor == -1) {
		mylastidx := rf.logs[len(rf.logs)-1].Index
		mylastterm := rf.logs[len(rf.logs)-1].Term
		if mylastterm < args.Log.Term || (mylastterm == args.Log.Term && mylastidx <= args.Log.Index) {
			reply.Voted = true
			rf.termChangeChnl <- args.Term
			rf.VotedFor = args.ID
			// rf.leaderBeginChnl <- false
			rf.isLeader.Set(false)
			rf.heartBeatTimerReset <- false
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	okChnl := make(chan bool, 1)
	go func() {
		okChnl <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}()
	var ok bool
	select {
	case ok = <-okChnl:
	case <-time.After(DefaultReqVoteTimeout()):
	}
	return ok

	// return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// AppendEntriesArgs struct is the heartbeat payload
type AppendEntriesArgs struct {
	Term            int32
	LeaderID        int32
	PrevLog         RaftLog
	Entries         []RaftLog
	LeaderCommitIdx int32
}

// AppendEntriesArgs struct is the heartbeat payload
type AppendEntriesReply struct {
	Term      int32
	Success   bool
	LastEntry RaftLog
}

// This is called by the leader to its followers.
// Followers will have their log entries updated and reply.
func (rf *Raft) HeartBeat(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rpcChnl <- OK
	currentTerm := rf.currentTerm.Load()
	reply.Term = currentTerm
	reply.LastEntry = rf.logs[len(rf.logs)-1]
	// DPrintf("%d: Args: %v", rf.me, args)

	if args.Term < currentTerm {
		// DPrintf("%d: 1", rf.me)
		return
	}
	// DPrintf("%d: HB rpc from %d term %d and me %d", rf.me, args.LeaderID, args.Term, currentTerm)
	if args.Term >= currentTerm {
		// DPrintf("%d: higher term HB rpc from %d", rf.me, args.LeaderID)
		rf.termChangeChnl <- args.Term
		rf.currentTerm.Set(args.Term)
		rf.isLeader.Set(false)
		rf.heartBeatTimerReset <- false
	}
	// Ignore the first log (index starts at 1)
	lengthLogs := len(rf.logs)
	prevLogIdx := int(args.PrevLog.Index)
	prevLogTerm := args.PrevLog.Term

	if prevLogIdx < lengthLogs && rf.logs[prevLogIdx].Term != prevLogTerm {
		// find the first entry of the wrong term
		conflictingTerm := rf.logs[prevLogIdx].Term
		currPrevLogIdx := prevLogIdx
		for rf.logs[currPrevLogIdx].Term == conflictingTerm && currPrevLogIdx != 1 {
			currPrevLogIdx--
		}
		currPrevLogIdx++
		rf.logs = rf.logs[:currPrevLogIdx]
		reply.LastEntry = rf.logs[currPrevLogIdx-1]
		rf.logidx.Set(reply.LastEntry.Index)
		DPrintf("%d: 2", rf.me)
		return
	} else if int32(prevLogIdx) > reply.LastEntry.Index {
		DPrintf("%d: 4, %v", rf.me, reply)
		return
	}

	for i, v := range args.Entries {
		if int(v.Index) == lengthLogs {
			// DPrintf("%d: frm %d, append rest from i:%d, t:%d", rf.me, args.LeaderID, v.Index, v.Term)
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		} else if int(v.Index) < lengthLogs && rf.logs[v.Index].Term != v.Term {
			rf.logs = append(rf.logs[:v.Index], args.Entries[i:]...)
			break
		} else if v.Index > reply.LastEntry.Index {
			DPrintf("%d: 3", rf.me)
			return
		}
	}
	reply.LastEntry = rf.logs[len(rf.logs)-1]

	rf.logidx.Set(reply.LastEntry.Index)
	oldCommitIdx := rf.commitIdx.Load()
	if args.LeaderCommitIdx > oldCommitIdx {
		newCommitIdx := func(a, b int32) int32 {
			if a < b {
				return a
			}
			return b
		}(reply.LastEntry.Index, args.LeaderCommitIdx)
		oldLastApplied := rf.lastApplied.Load()
		// DPrintf("%d: oldc:%d, oldA:%d, newC:%d", rf.me, oldCommitIdx, oldLastApplied, newCommitIdx)
		if newCommitIdx > oldLastApplied {
			rf.lastApplied.Set(newCommitIdx)
			rf.commitIdx.Set(newCommitIdx)
			toApply := rf.logs[oldLastApplied+1 : newCommitIdx+1]
			DPrintf("%d: Follower apply log from %d, %d, last: %v", rf.me, oldLastApplied+1, newCommitIdx+1, rf.logs[newCommitIdx])
			for _, v := range toApply {
				rf.applyChnl <- ApplyMsg{
					Index:       int(v.Index),
					Command:     v.Cmd,
					UseSnapshot: false,
					Snapshot:    nil,
				}
			}

		}
	}

	defer func() {
		// DPrintf("%d: hb frm %d, commitidx: ovt%dv%d, last Logs: %v ", rf.me, args.LeaderID, rf.commitIdx.Load(), args.LeaderCommitIdx, reply.LastEntry)
	}()
	rf.persist()
	if reply.LastEntry.Index < args.PrevLog.Index {
		DPrintf("%d: 5", rf.me)
		return
	}
	reply.Success = true
}

func (rf *Raft) sendHeartBeat(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	okChnl := make(chan bool, 1)
	go func() {
		okChnl <- rf.peers[server].Call("Raft.HeartBeat", args, reply)
	}()
	var ok bool
	select {
	case ok = <-okChnl:
		// DPrintf("%d: heart beat replied frm %d", rf.me, server)
	case <-time.After(DefaultHeartBeatTimeout()):
		// DPrintf("%d: %d no reply timeout\n", rf.me, server)
	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if !rf.isLeader.Load() {
		// DPrintf("%d: not leader", rf.me)
		return 0, 0, false
	}
	index := rf.logidx.Add(1)
	term := rf.currentTerm.Load()
	// start appending log entry
	// newLog := RaftLog{
	// 	Index: index,
	// 	Term:  term,
	// 	Cmd:   command,
	// }
	// fmt.Printf("%d: appending job %d, cmd %d\n", rf.me, index, command)
	rf.jobsChnl <- RaftLog{
		Index: index,
		Term:  term,
		Cmd:   command,
	}
	fmt.Printf("%d: job %d, cmd %d\n", rf.me, index, command)
	return int(index), int(term), true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.stopChnl <- true
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)
	// seed global random for each raft
	rand.Seed(int64(rf.me))

	// Your initialization code here.
	rf.commitIdx = &At32{}
	rf.currentTerm = &At32{}
	rf.lastApplied = &At32{}
	rf.logidx = &At32{}
	rf.isLeader = &AtBool{}
	rf.stopChnl = make(chan bool, 1)
	rf.logs = make([]RaftLog, 1)
	rf.rpcChnl = make(chan int, MAX_LOGS)
	rf.heartBeatChnl = make([]chan struct{}, len(rf.peers))
	for i := range rf.heartBeatChnl {
		rf.heartBeatChnl[i] = make(chan struct{}, 1)
	}
	rf.rdyChnl = make([]chan AppendEntriesArgs, len(rf.peers))
	for i := range rf.rdyChnl {
		rf.rdyChnl[i] = make(chan AppendEntriesArgs, MAX_LOGS)
	}
	rf.jobsChnl = make(chan RaftLog, MAX_LOGS)
	rf.nextIndex = make([]*At32, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = &At32{}
	}
	rf.matchIndex = make([]*At32, len(rf.peers))
	for i := range rf.nextIndex {
		rf.matchIndex[i] = &At32{}
	}
	rf.applyChnl = applyCh
	rf.VotedFor = -1
	rf.heartbeatTimer = time.NewTicker(DefaultHeartBeatTime())
	rf.heartbeatTimer.Stop()
	rf.heartBeatTimerReset = make(chan bool, 1)
	// rf.leaderBeginChnl = make(chan bool, 1)
	rf.termChangeChnl = make(chan int32, len(rf.peers))
	rf.commitIdxChnl = make(chan int32, 512)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Heartbeat timer routine
	go func(rf *Raft) {
		lengthPeers := len(rf.peers)
		hbtime := DefaultHeartBeatTime()
		DPrintf("%d: Begin", rf.me)
		for {
			select {
			case <-rf.stopChnl:
				rf.stopChnl <- true
				return
			default:
			}
			select {
			case r := <-rf.heartBeatTimerReset:
				rf.heartbeatTimer.Stop()
				// Drain the ticker channel
				select {
				case <-rf.heartbeatTimer.C:
				default:
				}
				if r {
					// DPrintf("%d: Hb time %d", rf.me, hbtime.Milliseconds())
					rf.heartbeatTimer = time.NewTicker(hbtime)
				}
				continue
			default:
			}
			select {
			case <-rf.heartbeatTimer.C:
				prevLog := rf.logs[len(rf.logs)-1]
				for i := 0; i < lengthPeers; i++ {
					// Stop overloading channel with heartbeat if
					// there are on-going client requests
					//
					if len(rf.rdyChnl[i]) != 0 || i == int(rf.me) {
						continue
					}
					appendArgs := AppendEntriesArgs{
						Term:            rf.currentTerm.Load(),
						LeaderID:        rf.me,
						PrevLog:         prevLog,
						Entries:         nil,
						LeaderCommitIdx: rf.commitIdx.Load(),
					}
					rf.rdyChnl[i] <- appendArgs
				}
			default:
			}

		}
	}(rf)

	// All incoming entries will go to jobschnl
	// before dissiminated to individual peer channels
	go func(rf *Raft) {
		maxEntriesPerBatch := 30
		for {
			select {
			case <-rf.stopChnl:
				rf.stopChnl <- true
				return
			case <-time.After(JobChannelPollRate()):
				select {
				case job := <-rf.jobsChnl:
					numJobs := len(rf.jobsChnl) + 1

					count := func(a, b int) int {
						if a < b {
							return a
						}
						return b
					}(numJobs, maxEntriesPerBatch)
					// DPrintf("%d: Before Joblog: %v", rf.me, rf.logs)
					prevLog := rf.logs[len(rf.logs)-1]
					var entries []RaftLog = make([]RaftLog, count)
					entries[0] = job
					// newLogIndex := len(rf.logs)
					for i := 1; i < count; i++ {
						j := <-rf.jobsChnl
						// DPrintf("%d: JIdx: %d, PrevLog:%d, count:%d", rf.me, j.Index, prevLog.Index, count)

						entries[j.Index-prevLog.Index-1] = j
					}

					// no need to send if not leader.
					if rf.isLeader.Load() {
						appendArgs := AppendEntriesArgs{
							Term:            rf.currentTerm.Load(),
							LeaderID:        rf.me,
							PrevLog:         prevLog,
							Entries:         entries,
							LeaderCommitIdx: rf.commitIdx.Load(),
						}
						for i := 0; i < len(rf.peers); i++ {
							if int(rf.me) == i {
								continue
							}
							rf.rdyChnl[i] <- appendArgs
						}
					}

					// Never replace. Append only
					for i, v := range entries {
						if int(v.Index) == len(rf.logs) {
							rf.logs = append(rf.logs, entries[i:]...)
							break
						} else if int(v.Index) < len(rf.logs) {
							continue
						}
					}
					rf.persist()
					// DPrintf("%d: Joblog: %v", rf.me, rf.logs)
				default:
				}
			}

		}

	}(rf)

	// Commit index updater
	go func(rf *Raft) {
		commitTracker := make(map[int32]int32)
		reqLen := int32(len(rf.peers) / 2)

		defer DPrintf("%d: Exited logs:%v", rf.me, rf.logs)
		for {
			select {
			case <-rf.stopChnl:
				rf.stopChnl <- true
				return
			case newIdx := <-rf.commitIdxChnl:
				// || rf.logs[newIdx].Term != rf.currentTerm.Load()
				lengthLogs := len(rf.logs)
				var currCommitIdx = rf.commitIdx.Load()
				if newIdx <= currCommitIdx || newIdx >= int32(lengthLogs) {
					continue
				}
				_, ok := commitTracker[newIdx]
				if !ok {
					commitTracker[newIdx] = 0
				}
				commitTracker[newIdx]++
				// for k, v := range commitTracker {
				// 	if k > currentMaxReqLenIndex && k <= newIdx {
				// 		commitTracker[k]++
				// 		v = commitTracker[k]
				// 	}
				// 	if v >= reqLen && k > currentMaxReqLenIndex {
				// 		currentMaxReqLenIndex = k
				// 	}
				// }
				DPrintf("%d: commitidx: %d, %d", rf.me, newIdx, commitTracker[newIdx])
				if commitTracker[newIdx] == reqLen && newIdx > currCommitIdx {
					endIdx := newIdx

					toApply := rf.logs[currCommitIdx+1 : endIdx+1]
					for _, log := range toApply {
						rf.applyChnl <- ApplyMsg{
							Index:       int(log.Index),
							Command:     log.Cmd,
							UseSnapshot: false,
							Snapshot:    nil,
						}
					}
					DPrintf("%d: apply %v", rf.me, toApply)
					rf.commitIdx.Set(endIdx)
					rf.lastApplied.Set(endIdx)
					rf.persist()
					// DPrintf("%d: Apply commit logs %v\ncidx:%d logs:%v", rf.me, toApply, commitIdx, rf.logs)
				}
			}

		}
	}(rf)

	// initialize peer job channel routine
	go func(rf *Raft) {
		for i := 0; i < len(rf.peers); i++ {
			if int(rf.me) == i {
				continue
			}
			// individual peer job channel loop
			go func(server int, rdyChnl <-chan AppendEntriesArgs) {
				for {
					select {
					case <-rf.stopChnl:
						rf.stopChnl <- true
						return
					case appendArg := <-rdyChnl:
						// Each job is wrapped in a function to allow defer to work
						// so that individual jobs are performed in synchronisation until
						// the channel is empty
						if !rf.isLeader.Load() {
							break
						}
						reply := &AppendEntriesReply{}

						nextIdx := rf.nextIndex[server].Load()
						if appendArg.PrevLog.Index >= nextIdx {
							appendArg.Entries = append(rf.logs[nextIdx:appendArg.PrevLog.Index+1], appendArg.Entries...)
							// DPrintf("%d: appendArgs:%v", rf.me, appendArg)
							appendArg.PrevLog = rf.logs[nextIdx-1]
						}
						// DPrintf("%d: HeartBeat send %d", rf.me, server)
						ok := rf.sendHeartBeat(server, appendArg, reply)

						if !ok {
							// RPC failed
							// DPrintf("%d: HeartBeat RPC failed send to %d", rf.me, server)
							break
						}
						// DPrintf("%d: HeartBeat reply frm %d %v", rf.me, server, reply)
						if reply.Success {
							// Reply  successful
							// DPrintf("%d: HeartBeat OK %d", rf.me, server)
							var lastEntryIdx = reply.LastEntry.Index
							serverLastMatch := rf.matchIndex[server].Load()
							rf.nextIndex[server].Set(lastEntryIdx + 1)

							if lastEntryIdx > serverLastMatch {
								// DPrintf("%d: from %d, lastEntry:%d, lastMatch:%d", rf.me, server, lastEntryIdx, serverLastMatch)
								rf.matchIndex[server].Set(lastEntryIdx)
								rf.commitIdxChnl <- lastEntryIdx
							}
						} else {
							//reply not successful
							// Retry ?

							if reply.Term > rf.currentTerm.Load() {
								rf.termChangeChnl <- reply.Term
								// rf.leaderBeginChnl <- false
								rf.currentTerm.Set(reply.Term)
								rf.isLeader.Set(false)
								rf.heartBeatTimerReset <- false
							} else {
								replyTerm := reply.LastEntry.Term
								replyIdx := reply.LastEntry.Index
								// DPrintf("%d: %d reply: %v, logs len: %d", rf.me, server, reply, len(rf.logs))
								if replyIdx > 1 && replyIdx <= appendArg.PrevLog.Index && rf.logs[replyIdx].Term != replyTerm {
									for rf.logs[replyIdx].Term >= appendArg.Term {
										replyIdx--
									}
									rf.nextIndex[server].Set(replyIdx + 1)

								} else {
									rf.nextIndex[server].Set(replyIdx + 1)
								}

							}
						}

					}
				}
			}(i, rf.rdyChnl[i])
		}
	}(rf)

	// raft loops
	go func(rf *Raft) {
		rf.electionTimer = time.NewTimer(DefaultElectionTimeout())
		lengthPeers := len(rf.peers)
		for {
			select {
			case <-rf.stopChnl:
				rf.stopChnl <- true
				return
			default:
			}
			// select {
			// case leader := <-rf.leaderBeginChnl:
			// 	if rf.isLeader.Load() && !leader {
			// 		DPrintf("%d: Reverted to follower", rf.me)
			// 		select {
			// 		case rf.rpcChnl <- OK:
			// 		default:
			// 		}
			// 	}
			// 	// rf.isLeader.Set(leader)
			// default:
			// }
			select {
			case <-rf.termChangeChnl:
				rf.persist()
			default:
			}

			select {
			case <-rf.rpcChnl:
				// DPrintf("%d: E Tiemout reset", rf.me)
				timeout := DefaultElectionTimeout()
				if !rf.electionTimer.Stop() {
					select {
					case <-rf.electionTimer.C:
					default:
					}
				}
				rf.electionTimer.Reset(timeout)
				continue
			default:
			}

			if rf.isLeader.Load() {
				continue
			}

			select {
			case <-rf.electionTimer.C:
				// begin election
				currentTerm := rf.currentTerm.Add(1)
				rf.VotedFor = rf.me
				lastLog := rf.logs[len(rf.logs)-1]
				commitIdx := rf.commitIdx.Load()
				rf.VotedFor = rf.me
				timeout := DefaultElectionTimeout()
				//send requestvotes rpc
				args := RequestVoteArgs{
					Term: currentTerm,
					ID:   rf.me,
					Log:  lastLog,
				}
				votingChnl := make(chan bool, len(rf.peers)-1)
				// DPrintf("%d: Election Term %d, next timeout %dms", rf.me, currentTerm, timeout.Milliseconds())
				rf.electionTimer.Reset(timeout)
				// Tracks the voting for this current election
				go func(currentTerm int32, votingChnl <-chan bool) {
					// Check votes
					votes := 0
					// internal timer will timeout with the current election
					internalTimer := time.NewTimer(timeout)
					defer internalTimer.Stop()
					for {
						select {
						case <-rf.stopChnl:
							rf.stopChnl <- true
							return
						case <-internalTimer.C:
							return
						case isVoted := <-votingChnl:
							if isVoted {
								votes++
								// DPrintf("%d: Votes %d,term %d", rf.me, votes, currentTerm)
							} else {
								// If a false appears, we will quit the voting session
								return
							}
							if votes == len(rf.peers)/2 {
								rf.heartBeatTimerReset <- true
								rf.isLeader.Set(true)
								// rf.leaderBeginChnl <- true
								// Becoming leader
								for i := 0; i < len(rf.peers); i++ {
									if int(rf.me) == i {
										continue
									}
									rf.rdyChnl[i] <- AppendEntriesArgs{
										Term:            currentTerm,
										LeaderID:        rf.me,
										PrevLog:         lastLog,
										Entries:         nil,
										LeaderCommitIdx: commitIdx,
									}
								}
								for i := range rf.peers {
									rf.nextIndex[i].Set(int32(len(rf.logs)))
									rf.matchIndex[i].Set(0)
								}
								// DPrintf("%d: Leader term %d", rf.me, currentTerm)
								return
							}
						}

					}

				}(currentTerm, votingChnl)

				for i := 0; i < lengthPeers; i++ {
					if i != int(rf.me) {
						// request vote rpc for each peer that is not me
						go func(server int, args RequestVoteArgs, votingChnl chan bool) {
							reply := &RequestVoteReply{}
							ok := rf.sendRequestVote(server, args, reply)
							if !ok {
								return
							}
							// var retries = 3
							// done := make(chan int, 1)
							// go func() {
							// 	for i := 0; i < retries; i++ {
							// 		ok := rf.sendRequestVote(server, args, reply)
							// 		if ok {
							// 			done <- OK
							// 			break
							// 		}
							// 		// DPrintf("%d: Failed ReqVote rpc to %d, retry...", rf.me, server)
							// 	}
							// 	done <- NOT_OK
							// }()

							// if <-done == NOT_OK {
							// 	// RPC Failed
							// 	// DPrintf("%d: RequestVoteRPC failed %d t%d", rf.me, server, args.Term)
							// 	return
							// }
							// Send true votes or error votes
							if reply.Voted {
								votingChnl <- reply.Voted
							} else if reply.Term > rf.currentTerm.Load() {
								// revert to follower
								// stop the hb timer, if it is running
								rf.heartBeatTimerReset <- false
								rf.termChangeChnl <- reply.Term
								rf.currentTerm.Set(reply.Term)
								rf.isLeader.Set(false)
								// rf.leaderBeginChnl <- false
								votingChnl <- reply.Voted
								// DPrintf("%d: Higher term seen %dt server:%d", rf.me, reply.Term, server)
							}
						}(i, args, votingChnl)
					}
				}

			default:
			}

		}
	}(rf)

	return rf
}

//DefaultElectionTimeout default
func DefaultElectionTimeout() time.Duration {
	return GetRandDuration(500, 900, time.Millisecond)
}

func DefaultHeartBeatTimeout() time.Duration {
	return 5 * DefaultHeartBeatTime()
}

func DefaultReqVoteTimeout() time.Duration {
	return 300 * time.Millisecond
}

func DefaultHeartBeatTime() time.Duration {
	return 12 * time.Millisecond
}

func JobChannelPollRate() time.Duration {
	return 2 * time.Millisecond
}

// GetRandDuration randomise between [begin, end) milliseconds
func GetRandDuration(begin, end int32, duration time.Duration) time.Duration {
	return time.Duration(rand.Int31n(end-begin)+begin) * duration
}
