package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false
const GetOp = "Get"
const PutOp = "Put"
const AppendOp = "Append"


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	Key string
	Value string

	ClientId int64
	RequestId int
}



type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied int
	// Your definitions here.
	lastOperations map[int64]int
	notifyChans map[int]chan Notification

	DB map[string]string
}

type Notification struct {
	ClientId  int64
	RequestId int
}

func (kv *KVServer) shouldSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false 
	}
	
	if kv.rf.GetRaftStateSize() >= kv.maxraftstate{
		return true
	}
	return false
}

func (kv *KVServer) takeSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.DB)
	e.Encode(kv.lastOperations)
	lastApplied := kv.lastApplied
	kv.mu.Unlock()
	log.Printf("kv takesnapshot ***********")
	kv.rf.Snapshot(lastApplied, w.Bytes())
}

func (kv *KVServer) isDuplicateRequest(clinetId int64, requestId int) bool  {
	appliedRequestId, ok := kv.lastOperations[clinetId]
	if ok == false || requestId > appliedRequestId {
		return false 
	}
	return true
}

func (kv *KVServer) waitApplying(op Op, timeout time.Duration) bool {
	//log.Printf("waitapply start")
	index, _, isleader := kv.rf.Start(op)
	//log.Printf("isleader %v index %d", isleader, index)
	if isleader == false {
		return true
	}

	if kv.shouldSnapshot() {
		kv.takeSnapShot()
	}

	var WrongLeader bool 

	kv.mu.Lock()
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan Notification, 1)
	}
	ch := kv.notifyChans[index]
	kv.mu.Unlock()

	select{
	case notify := <-ch:
		if notify.ClientId != op.ClientId || notify.RequestId != op.RequestId {
			WrongLeader = true
		}else {
			WrongLeader = false
		}

	case <- time.After(timeout):
		kv.mu.Lock()
		if kv.isDuplicateRequest(op.ClientId, op.RequestId){
			WrongLeader = false
		}else {
			WrongLeader = true
		}
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	delete(kv.notifyChans, index)
	kv.mu.Unlock()
	return WrongLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{
		Command : GetOp,
		Key : args.Key,
		Value : "",
		ClientId : args.ClientId,
		RequestId : args.RequestId,
	}
	
	WrongLeader := kv.waitApplying(op, 500*time.Millisecond)

	if WrongLeader == false {
		kv.mu.Lock()
		value, ok := kv.DB[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = value
			return
		}
		reply.Err = ErrNoKey
	} else {
		reply.Err = ErrWrongLeader
	}
	
	//log.Printf("client %d get Get reply %v", args.ClientId, reply)
	
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Command : args.Op,
		Key : args.Key,
		Value : args.Value,
		ClientId : args.ClientId,
		RequestId : args.RequestId,
	}
	
	WrongLeader := kv.waitApplying(op, 500*time.Millisecond)
	if WrongLeader{
		reply.Err = ErrWrongLeader
	}
	//log.Printf("client %d get putappend reply %v", args.ClientId, reply)
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <- kv.applyCh:
			
			if msg.CommandValid {
				log.Printf("raft[%d] try to apply message %v | kv.lastapplied %d msg command index %d", kv.rf.Me(), msg, kv.lastApplied, msg.CommandIndex)
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					log.Printf("outdated message %v", msg)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				if op.Command != GetOp && kv.isDuplicateRequest(op.ClientId, op.RequestId){
					log.Printf("duplicate message %v", msg)
					kv.mu.Unlock()
					continue
				} 
				switch op.Command {
				case "Put":
					kv.DB[op.Key] = op.Value
				case "Append":
					kv.DB[op.Key] += op.Value
					//log.Printf("kv %d DB[0] len %d", kv.me, len(kv.DB["0"]))
				}
				kv.lastApplied = msg.CommandIndex
				kv.lastOperations[op.ClientId] = op.RequestId

				if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
					notify := Notification{
						ClientId : op.ClientId,
						RequestId : op.RequestId,
					}
					ch <- notify
				}

				kv.mu.Unlock()
				if kv.shouldSnapshot() {
					kv.takeSnapShot()
				}
			
			} else if msg.SnapshotValid{
				//log.Printf("raft[%d] try to apply snapshot msg  ^_^", kv.rf.Me())
				kv.InstallSnapshot(msg.Snapshot)
				//log.Printf("%s", kv.DB["0"])
			} else {
				log.Printf("unexpected message")
			}
		
		}
	}
}

func (kv *KVServer) InstallSnapshot(snapshot []byte) {
	if snapshot != nil {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var db map[string]string
		var lastop map[int64]int
		if d.Decode(&db) != nil || d.Decode(&lastop) != nil {
			log.Printf("kvsever %d fail to recover", kv.me)
		}else {
			kv.mu.Lock()
			kv.DB = db
			kv.lastOperations = lastop
			kv.mu.Unlock()
			log.Printf("kvsever %d success to recover", kv.me)
		}
	} 
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.DB = make(map[string]string)
	kv.lastOperations = make(map[int64]int)
	kv.notifyChans = make(map[int]chan Notification)
	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	kv.InstallSnapshot(snapshot)

	// You may need initialization code here.
	go kv.applier()
	return kv
}
