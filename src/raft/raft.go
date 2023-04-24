package raft



import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
)

const HEARTBEAT int = 300
const TIMEOUTLOW float64 = 500
const TIMEOUTHIGH float64 = 1000
const CHECKPERIOD float64 = 300
const FOLLOWER int = 0
const CANDIDATE int = 1
const LEADER int = 2


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state     int
	currentTerm int
	votedFor int
	logs     []LogEntry

	applyCh chan ApplyMsg
	replicatorCond []*sync.Cond
	applyCond *sync.Cond
	commitIndex int
	lastApplied int

	electionTimer *time.Timer
	heartbeatTimer *time.Timer

	nextIndex []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	snapshot := rf.persister.ReadSnapshot()
	Data := w.Bytes()
	rf.persister.Save(Data, snapshot)
	//DPrintf(dLog, "raft[%d] presisiter term%d votefor%d loglen%d", rf.me, rf.currentTerm, rf.votedFor,  len(rf.logs) - 1)
}

func (rf *Raft) HasLogInCurrentTerm()bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.state == LEADER {
		return rf.getLastlog().Term == rf.currentTerm
	}else{
		return true
	} 
}

func (rf *Raft) Me() int{
	return rf.me
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votefor int
	var logs []LogEntry
	if d.Decode(&term) != nil || d.Decode(&votefor) != nil || d.Decode(&logs) != nil  {
		DPrintf(dLog, "Error: raft%d readPersist.", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm = term
		rf.votedFor = votefor
		rf.logs = logs

		//DPrintf(dLog, "raft[%d] read presisiter term%d votefor%d loglen%d", rf.me, rf.currentTerm, rf.votedFor,  len(rf.logs) - 1)
		rf.mu.Unlock()
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	Data := w.Bytes()
	return Data
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		return 
	}
	rf.logs = TrimLogEntry(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
	DPrintf(dLog, "term : %d | raft[%d] accept the Snapshot from service | index %d", rf.currentTerm, rf.me, index)
	/*
	for _, log := range rf.logs {
		DPrintf(dLog, "raft[%d] have the log | index %d term %d ", rf.me, log.Index, log.Term)
	}
	*/
	//DPrintf(dLog, "term : %d | raft[%d] persister snapshot %v", rf.currentTerm, rf.me, rf.persister.ReadSnapshot())
}



type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	XTerm int
	XIndex int
}

type RequestVoteReply struct {
	Term        int 
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term int 
	LeaderId int 
	LastIncludedTerm int
	LastIncludedIndex int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func Max(a int, b int) int {
	if a > b {
		return a
	}else {
		return b
	}
}

func Min(a int, b int) int {
	if a > b {
		return b
	}else {
		return a
	}
}

func TrimLogEntry(logs []LogEntry) []LogEntry {
	entires := make([]LogEntry, 0)
	entires = append(entires, logs...)
	return entires
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//DPrintf(dLog, "term : %d | raft[%d] accept the requestvote from raft[%d]", rf.currentTerm, rf.me, args.CandidateId)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false 
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertstate(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if !rf.isLoguptodate(rf.getLastlog().Term, rf.getLastlog().Index, args.LastLogTerm, args.LastLogIndex){
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.electionTimer.Reset(time.Duration(randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH)) * time.Millisecond)
	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

func (rf *Raft) isLoguptodate(lastlogterm1 int, lastlogindex1 int, lastlogterm2 int, lastlogindex2 int) bool {
	//DPrintf(dLog, "term : %d | raft[%d] (lastlogterm %d lastlogindex %d)accept the requestvote from raft[%d] (lastlogterm %d lastlogindex %d) ", rf.currentTerm, rf.me,lastlogterm1, lastlogindex1, 8, lastlogterm2, lastlogindex2)
	if lastlogterm1 != lastlogterm2 {
		return lastlogterm1 < lastlogterm2
	}else  {
		return lastlogindex1 <= lastlogindex2 
	}
}

//接受日志 handl appendentry
func (rf *Raft) ReciveAppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if len(args.Entries) == 0 {
		//DPrintf(dLog, "term : %d | raft[%d] accept the heartbeat from raft[%d]", rf.currentTerm, rf.me, args.LeaderId)
	}else {
		//DPrintf(dLog, "term : %d | raft[%d] accept the appendentry from raft[%d]", rf.currentTerm, rf.me, args.LeaderId)
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		return 
	}
	if args.Term > rf.currentTerm {
		//DPrintf(dLog, "term : %d | raft[%d] start follower convert", rf.currentTerm, rf.me)
		rf.convertstate(FOLLOWER)
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(time.Duration(randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH)) * time.Millisecond)


	
	firstlogindex := rf.getFirstLog().Index
	lastlogindex := rf.getLastlog().Index

	if args.PreLogIndex > lastlogindex {
		reply.Success = false
		reply.XTerm, reply.XIndex = -1, lastlogindex + 1
		return 
	}
	if args.PreLogIndex < firstlogindex {
		reply.Term, reply.Success = 0, false
		return 
	}
	if rf.logs[args.PreLogIndex - firstlogindex].Term != args.PreLogTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		replyindex := args.PreLogIndex - 1 
		reply.XTerm = rf.logs[args.PreLogIndex - firstlogindex].Term
		for replyindex - firstlogindex - 1 > 0 && rf.logs[replyindex - firstlogindex - 1].Term == reply.XTerm {
			replyindex --
		} 
		reply.XIndex = replyindex
		return 
	}
	/*
	for _, log := range rf.logs {
		DPrintf(dLog, "raft[%d] have the log | index %d term %d ", rf.me, log.Index, log.Term)
	}
	*/
	
	for index, entry := range args.Entries {
		//DPrintf(dLog, "raft[%d] | entry index %d", rf.me, entry.Index)
		if entry.Index - firstlogindex > len(rf.logs) - 1 || rf.logs[entry.Index - firstlogindex].Term != entry.Term {
			rf.logs = append(rf.logs[0 : entry.Index - firstlogindex], args.Entries[index:]...)
			//DPrintf(dLog, "raft[%d] append the logs index from %d to %d", rf.me, lastlogindex, rf.getLastlog().Index)
			break
		} 
	}

	/*
	for _, log := range rf.logs {
		DPrintf(dLog, "raft[%d] have the log | index %d term %d command %v ", rf.me, log.Index, log.Term, log.Command)
	}
	*/

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastlog().Index)
		rf.lastApplied = Max(rf.lastApplied, rf.getFirstLog().Index)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
	return
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return 
	} 
	if args.Term > rf.currentTerm {
		rf.convertstate(FOLLOWER)
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.electionTimer.Reset(time.Duration(randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH)) * time.Millisecond)
	//DPrintf(dLog, "term : %d | raft[%d] accept the Installsnapshot from raft[%d]", rf.currentTerm, rf.me, args.LeaderId)
	
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return 
	}
	if args.LastIncludedIndex < rf.getFirstLog().Index {
		rf.mu.Unlock()
		return 
	}

	if args.LastIncludedIndex > rf.getLastlog().Index {
		rf.logs = make([]LogEntry, 1)
	}else {
		rf.logs = TrimLogEntry(rf.logs[args.LastIncludedIndex - rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term, rf.logs[0].Index = args.LastIncludedTerm, args.LastIncludedIndex
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex
	/*
	DPrintf(dLog, "term : %d | raft[%d] accept the Installsnapshot from raft[%d] shot index %d", rf.currentTerm, rf.me, args.LeaderId, rf.logs[0].Index)

	for _, log := range rf.logs {
		DPrintf(dLog, "raft[%d] have the log | index %d term %d ", rf.me, log.Index, log.Term)
	}
	
	DPrintf(dLog, "raft[%d] snapshot msg %v ", rf.me, args.Data)
	*/
	rf.persister.Save(rf.encodeState(), args.Data)
	
	msg := ApplyMsg {
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.mu.Unlock()
	//DPrintf(dLog, "term : %d | raft[%d] send snapshot apply ", rf.currentTerm, rf.me)
	
	rf.applyCh <- msg

}



func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReciveAppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf(dLog, "raft[%d] Start() state : %d ", rf.me, rf.state)
	if rf.state != LEADER {
		return -1, -1, false;
	}
	//DPrintf(dLog, "raft[%d] accept new command %v ", rf.me, command)
	newLog := rf.appendNewEntry(command)
	rf.heartbeat(false)
	// Your code here (2B).


	return newLog.Index, newLog.Term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) appendNewEntry(command interface{}) LogEntry {
	newLog := LogEntry {
		Term : rf.currentTerm,
		Index : rf.getLastlog().Index + 1,
		Command : command,
	}
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	//DPrintf(dLog, "raft[%d] append newlog index:%d  term:%d | command : %v", rf.me, newLog.Index, newLog.Term, newLog.Command)

	return newLog
}

func (rf *Raft) getLastlog() LogEntry {
	return rf.logs[len(rf.logs) - 1]
}
func (rf *Raft) getFirstLog() LogEntry{
	return rf.logs[0]
}

func randTimeDuration(low float64, high float64) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	timeout := int(r.Float64()*(TIMEOUTHIGH-TIMEOUTLOW) + TIMEOUTLOW)
	return timeout
}

//心跳
func (rf *Raft) heartbeat(isheartbeat bool){
		if rf.killed() == true {
			return 
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if isheartbeat == true {
				//DPrintf(dLog, "heartbeat | raft[%d] send appendentry / heartbeat to raft[%d]",rf.me, server)
				go rf.replicatorOneRound(server)
			}else {
				rf.replicatorCond[server].Signal()
			}
		}
}

//复制日志发送
func (rf *Raft) replicatorOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}
	firstlogindex := rf.getFirstLog().Index
	leaderid := rf.me
	term := rf.currentTerm
	prelogindex := rf.nextIndex[peer] - 1
	entries := make([]LogEntry, 0)
	//entries = append(entries, rf.logs[prelogindex - firstlogindex + 1:]...)
	//commitindex := rf.commitIndex

	if prelogindex < firstlogindex {
		lastincludedterm := rf.getFirstLog().Term
		lastincludedindex := rf.getFirstLog().Index
		data := rf.persister.ReadSnapshot()
		rf.mu.RUnlock()
		//DPrintf(dLog, "raft[%d] send installsnapshot to raft[%d] | data %v", rf.me, peer, rf.persister.ReadSnapshot())
		rf.callInstallSnapshot(peer, leaderid, term, lastincludedterm, lastincludedindex, data)
	} else {
		commitindex := rf.commitIndex
		entries = append(entries, rf.logs[prelogindex - firstlogindex + 1:]...)
		prelogterm := rf.logs[prelogindex - firstlogindex].Term
		rf.mu.RUnlock()
		rf.callAppendEntries(peer, leaderid, term, prelogindex, prelogterm, entries, commitindex)
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			//DPrintf(dLog, "raft[%d] replicator wait", rf.me)
			rf.replicatorCond[peer].Wait()
		}
		//DPrintf(dLog, "raft[%d] replicator start", rf.me)
		//DPrintf(dLog, "replicator | raft[%d] send appendentry / heartbeat to raft[%d]",rf.me, peer)
		rf.replicatorOneRound(peer)
	}
}


//判断复制日志条件
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == LEADER && rf.matchIndex[peer] < rf.getLastlog().Index
}


// call appendentries and obtain the reply
func (rf * Raft)callAppendEntries(server int, leaderid int, term int, prelogindex int, prelogterm int, entries []LogEntry, leadercommit int) bool {
	args := AppendEntriesArgs{
		Term : term,
		LeaderId : leaderid,
		PreLogIndex : prelogindex,
		PreLogTerm : prelogterm,
		Entries : entries,
		LeaderCommit : leadercommit,
	}
	/*
	if len(entries) == 0 {
		DPrintf(dLog, "raft[%d] sent heartbeat to raft[%d]", rf.me, server)
	}else {
		DPrintf(dLog, "raft[%d] sent AppendEntry to raft[%d]", rf.me, server)
	}
	*/

	reply := AppendEntriesReply{}
	rf.sendAppendEntries(server, &args, &reply)
	

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.convertstate(FOLLOWER)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
	} 

	if reply.Success == true {
		newnext := args.PreLogIndex + len(entries) + 1
		newmatch := args.PreLogIndex + len(entries)
		if newnext > rf.nextIndex[server] {
			rf.nextIndex[server] = newnext
		}
		if newmatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newmatch
		}
		
			rf.advanceCommit()
	}else if reply.XIndex > 0 {
			rf.nextIndex[server] = reply.XIndex
	}
	
	//DPrintf(dLog, "raft[%d] handl the Entry reply from raft[%d] | reply %v", rf.me, server, reply)
	return reply.Success
}

func (rf * Raft)callInstallSnapshot(server int, leaderid int, term int, lastincludedterm int, lastincludedindex int, data []byte) {
	args := InstallSnapshotArgs {
		Term : term,
		LeaderId : leaderid,
		LastIncludedTerm : lastincludedterm,
		LastIncludedIndex : lastincludedindex,
		Data : data,
	}
	//DPrintf(dLog, "raft[%d] send installSnapshot to raft[%d] | args %v", rf.me, server,	args)
	reply := InstallSnapshotReply {}
	if rf.sendInstallSnapshot(server, &args, &reply){

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.convertstate(FOLLOWER)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}else {
			if rf.matchIndex[server] < args.LastIncludedIndex {
				rf.matchIndex[server] = args.LastIncludedIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} 
		rf.mu.Unlock()
	}
	return 
}

func (rf *Raft) advanceCommit() {
	if rf.state != LEADER {
		return 
	}
	firstlogindex := rf.getFirstLog().Index
	rf.commitIndex = Max(rf.commitIndex, firstlogindex)
	start := rf.commitIndex + 1
	for index := start; index <= rf.getFirstLog().Index + len(rf.logs) - 1; index ++ {
		if rf.logs[index - firstlogindex].Term != rf.currentTerm {
			continue
		}
		n := 1
		for i := 0; i < len(rf.peers); i ++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				n += 1
			}
		}
		if n > len(rf.peers) / 2 {
			rf.commitIndex = index
		}
	}
	rf.lastApplied = Max(rf.lastApplied, firstlogindex)
	rf.applyCond.Broadcast()
	//DPrintf(dLog, "raft[%d] applyCond send a signal() to applier", rf.me)
	return 
}

//发送request并处理reply
func (rf *Raft)callRequestVote(server int, term int, lastlogindex int, lastlogterm int) bool{
	args := RequestVoteArgs{
		Term : term,
		CandidateId : rf.me,
		LastLogIndex : lastlogindex,
		LastLogTerm : lastlogterm,
	}
	//DPrintf(dLog, "raft[%d] sent requestvote to raft[%d]", rf.me, server)
	reply := RequestVoteReply{}
	rf.sendRequestVote(server, &args, &reply)
	
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf(dLog, "raft[%d] handl the vote reply from raft[%d] | reply %v", rf.me, server, reply)
	if term != rf.currentTerm{
		return false 
	}
	if reply.Term > rf.currentTerm {
		rf.convertstate(FOLLOWER)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
	}
	return reply.VoteGranted
}

//开始选举
func (rf *Raft) startElection() {
	
	if rf.killed() == true {
		return 
	}
	rf.electionTimer.Reset(time.Duration(randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH)) * time.Millisecond)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	term := rf.currentTerm
	lastlogindex := rf.getLastlog().Index
	lastlogterm := rf.logs[lastlogindex - rf.getFirstLog().Index].Term
	rf.persist()

	//DPrintf(dLog, " term : %d | raft[%d][state : %d] start a election \n", rf.currentTerm, rf.me, rf.state)
	votes := 1
	votefinished := false
	var voteMutex sync.Mutex

	for peer := range rf.peers {
		if peer == rf.me {
			
			//DPrintf(dLog, "vote for self raft[%d]\n", rf.me)
			continue
		}

		go func (peer int){
			votegranted := rf.callRequestVote(peer, term, lastlogindex, lastlogterm)
			voteMutex.Lock()
			if votegranted && !votefinished {
				votes ++  
				if votes * 2 > len(rf.peers){
					votefinished = true
					rf.mu.Lock()
					rf.convertstate(LEADER)
					for i := 0; i < len(rf.peers); i ++ {
						rf.nextIndex[i] = rf.getLastlog().Index + 1
						rf.matchIndex[i] = 0;
					}
					rf.mu.Unlock()
					return 
				}
			}
			voteMutex.Unlock()
		}(peer)
	}

}

//状态转换
func (rf *Raft) convertstate(state int){

	if state == rf.state {
		return 
	}
	//DPrintf(dLog, "Term %d | raft[%d] convert from %v to %v\n",
	//	rf.currentTerm, rf.me, rf.state, state)
	rf.state = state
	switch state{
	case FOLLOWER:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(time.Duration(randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH)) * time.Millisecond)
		rf.votedFor = -1

	case CANDIDATE:
		rf.startElection()
	
	case LEADER:
		rf.electionTimer.Stop()
		rf.heartbeat(true)
		rf.heartbeatTimer.Reset(time.Duration(HEARTBEAT) * time.Millisecond)
	}
}

//上传 
func (rf *Raft) applier() {
	for rf.killed() == false  {
		rf.mu.Lock()
		//DPrintf(dLog, "raft[%d] applier start", rf.me)
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		//DPrintf(dLog, "raft[%d] applier start firstIndex %d commitIndex %d lastApplied %d", rf.me, firstIndex, commitIndex, lastApplied)
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			applyMsg := ApplyMsg {
				CommandValid : true,
				Command : entry.Command,
				CommandIndex : entry.Index,
			}
			rf.applyCh <- applyMsg
		}
		rf.mu.Lock()
		//DPrintf(dLog, "raft[%d] applies entries %d-%d in term %d", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
		
		
	}
}

// 监听
func (rf *Raft) ticker() {
	for rf.killed() == false {
		//DPrintf(dLog, "raft[%d] start | state : %d", rf.me, rf.state)
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == FOLLOWER {
				//DPrintf(dLog, "state %d Term %d | raft[%d] election timeout", rf.state, rf.currentTerm, rf.me)
				rf.convertstate(CANDIDATE)
			}else if rf.state == CANDIDATE {
				//DPrintf(dLog, "state %d Term %d | raft[%d] election timeout", rf.state, rf.currentTerm, rf.me)
				rf.startElection()
			}
			rf.mu.Unlock()

		case  <-rf.heartbeatTimer.C:
			DPrintf(dLog, "Term %d state : %d | raft[%d] heartbeat timeout", rf.currentTerm, rf.state, rf.me)
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.heartbeat(true)
				rf.heartbeatTimer.Reset(time.Duration(HEARTBEAT) * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers : peers,
		persister : persister,
		me : me,

		state : FOLLOWER,
		currentTerm : 0,
		commitIndex : 0,
		lastApplied : 0,
		votedFor : -1,
		logs : make([]LogEntry, 0),
		applyCh : applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)), 
		electionTimer : time.NewTimer(time.Duration(randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH)) * time.Millisecond),
		heartbeatTimer : time.NewTimer(time.Duration(HEARTBEAT) * time.Microsecond),
		matchIndex : make([]int, len(peers)),
		nextIndex : make([]int, len(peers)),
	}
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	//DPrintf(dLog, "start raft[%d] | electionTimeout = %v ", rf.me, randTimeDuration(TIMEOUTLOW, TIMEOUTHIGH))
	// initialize from state persisted before a crash
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, len(rf.logs)
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.heartbeatTimer.Stop()
	//DPrintf(dLog, "raft[%d] start | state : %d | Term %d", rf.me, rf.state, rf.currentTerm)
	go rf.ticker()
	go rf.applier()
	return rf
}
