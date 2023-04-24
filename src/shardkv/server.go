package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "6.5840/shardctrler"
import "fmt"
import "log"
import "bytes"
import "sync/atomic"
import "time"

const (
	ConfigureMonitorTimeout int = 50
	MigrationMonitorTimeout int = 80
	GCMonitorTimeout        int = 100
	EmptyEntryDetectorTimeout int = 30
)


type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

type Command struct {
	Op CommandType
	Data interface{}
}

type ShardOperationRequest struct {
	ConfigNum int
	ShardIds  []int
}

type ShardOperationResponse struct {
	Err Err
	ConfigNum int
	Shards map[int]map[string]string
	LastOperations map[int64]OperationContext
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(args *CommandArgs) Command {
	return Command{Operation, *args}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(reply *ShardOperationResponse) Command {
	return Command{InsertShards, *reply}
}

func NewDeleteShardsCommand(args *ShardOperationRequest) Command {
	return Command{DeleteShards, *args}
}

func NewEmptyEntryCommand() Command {
	return Command{EmptyEntry, nil}
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)


type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	dead 		 int32
		 
	gid          int

	//ctrlers      []*labrpc.ClientEnd

	sc           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	db         map[int]*Shard
	lastConfig   shardctrler.Config
	currentConfig shardctrler.Config

	lastOperations map[int64]OperationContext

	notifyChans  map[int]chan *CommandReply
	// Your definitions here.
}

type OperationContext struct {
	CommandId int
	LastReply *CommandReply
}

func (operation *OperationContext) deepCopy() OperationContext {
	tmp := OperationContext{}
	tmp.CommandId = operation.CommandId
	tmp.LastReply = operation.LastReply
	return tmp
}

type Shard struct {
	KV map[string]string
	Status ShardStatus
}

func NewShard() *Shard{
	return &Shard{make(map[string]string), Serving}
}

func (shard *Shard) Get(key string) (Err, string) {
	if value, ok := shard.KV[key]; ok {
		return OK, value
	}
	return ErrNoKey, ""
}

func (shard *Shard) Put(key string, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key string, value string) Err {
	shard.KV[key] += value
	return OK
}


func (shard *Shard) deepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}

func (kv *ShardKV) shouldSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false 
	}
	
	if kv.rf.GetRaftStateSize() >= kv.maxraftstate{
		return true
	}
	return false
}

func (kv *ShardKV) takeSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.lastOperations)
	lastApplied := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.Snapshot(lastApplied, w.Bytes())
}

func (kv *ShardKV) InstallSnapshot(snapshot []byte) {
	if snapshot != nil {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var db map[int]*Shard
		var lastop map[int64]OperationContext
		if d.Decode(&db) != nil || d.Decode(&lastop) != nil {
			log.Printf("kvsever %d fail to recover", kv.me)
		}else {
			kv.mu.Lock()
			kv.db = db
			kv.lastOperations = lastop
			kv.mu.Unlock()
			log.Printf("kvsever %d success to recover", kv.me)
		}
	} 
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) isDuplicateRequest(clinetId int64, requestId int) bool  {
	operation, ok := kv.lastOperations[clinetId]
	if ok == false {
		return false
	}
	appliedRequestId := operation.CommandId
	if  requestId > appliedRequestId {
		return false 
	}
	return true
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	log.Printf("raft[%d]{Group %v} accept command %v ", kv.rf.Me(), kv.gid, args)
	kv.mu.Lock()
	if !kv.canServe(key2shard(args.Key)){
	//	log.Printf("raft[%d]{Group %v} ready return wrongGroup", kv.rf.Me(), kv.gid)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return 
	}
	kv.mu.Unlock()

	kv.Execute(NewOperationCommand(args), reply)
}



func (kv *ShardKV) Execute(command Command, reply *CommandReply) {
	index, _, isleader := kv.rf.Start(command)
	if !isleader {
		reply.Err = ErrWrongLeader
		return 
	}
	
	if kv.shouldSnapshot() {
		kv.takeSnapShot()
	}
	
	kv.mu.Lock()
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	ch := kv.notifyChans[index]
	kv.mu.Unlock()
//	log.Printf("raft[%d]{Group %v} create a ch", kv.rf.Me(), kv.gid)
	select{
	case notify := <-ch:
		reply.Value, reply.Err = notify.Value, notify.Err
	case <- time.After(500*time.Millisecond):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.notifyChans, index)
	kv.mu.Unlock()
}

func(kv *ShardKV) applier(){
	for kv.killed() == false {
		select {
		case msg := <- kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue 
				}
				kv.lastApplied = msg.CommandIndex
				var reply *CommandReply
				command := msg.Command.(Command)
				//log.Printf("raft[%d]{Group %v} apply command %v", kv.rf.Me(), kv.gid, command)
				switch command.Op {
				case Operation:
					operation := command.Data.(CommandArgs)
					reply = kv.applyOperation(&msg, &operation)
				case Configuration:
					nextConfig := command.Data.(shardctrler.Config)
					reply = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationResponse)
					reply = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					shardsInfo := command.Data.(ShardOperationRequest)
					reply = kv.applyDeleteShards(&shardsInfo)
				case EmptyEntry:
					reply = kv.applyEmptyEnty()
				}
				
				if ch, ok := kv.notifyChans[msg.CommandIndex]; ok {
					ch <- reply
				}
				kv.mu.Unlock()
			//	log.Printf("raft[%d]{Group %v} | after apply ch", kv.rf.Me(), kv.gid)
				if kv.shouldSnapshot() {
					kv.takeSnapShot()
				}
			} else if msg.SnapshotValid {
				kv.InstallSnapshot(msg.Snapshot)
			}else {
				log.Printf("unexpected msg %v", msg)
			}
		}
	}
}

func (kv *ShardKV) canServe(shardId int) bool {
	//log.Printf("raft[%d]{Group %v} shard %d config %v ^_^", kv.rf.Me(), kv.gid, shardId, kv.currentConfig)
	return kv.currentConfig.Shards[shardId] == kv.gid && (kv.db[shardId].Status == Serving || kv.db[shardId].Status == GCing)
}

func (kv *ShardKV) applyOperation (msg *raft.ApplyMsg, args *CommandArgs) *CommandReply {
	var reply *CommandReply 
	shardId := key2shard(args.Key)
	if kv.canServe(shardId){
		if args.Op != GetOp && kv.isDuplicateRequest(args.ClientId, args.CommandId){
			return kv.lastOperations[args.ClientId].LastReply
		}else {
			reply = kv.applyToDb(args, shardId)
			if args.Op != GetOp{
				kv.lastOperations[args.ClientId] = OperationContext{args.CommandId, reply}
			}
			return reply
		}

	} 
	return &CommandReply{"", ErrWrongGroup}
}

func (kv *ShardKV) applyToDb(args *CommandArgs, shardId int) *CommandReply {
	var reply *CommandReply
	reply = &CommandReply{}
	switch args.Op{
	case GetOp:
		reply.Err, reply.Value = kv.db[shardId].Get(args.Key)
	case PutOp:
		reply.Err = kv.db[shardId].Put(args.Key, args.Value)
	case AppendOp:
		reply.Err = kv.db[shardId].Append(args.Key, args.Value)
	}

	return reply
}

func (kv *ShardKV) configureAction(){
	canPerformNextConfig := true
	kv.mu.RLock()
	for _, shard := range kv.db {
		if shard.Status != Serving {
			canPerformNextConfig = false 
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	if canPerformNextConfig {
		//log.Printf("raft[%d]{Group %v} start configure send", kv.rf.Me(), kv.gid)
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		//log.Printf("raft[%d]{Group %v} nextconfig %v", kv.rf.Me(), kv.gid, nextConfig)
		if nextConfig.Num == currentConfigNum + 1 {
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandReply{})
		}
	}
}

func Group2Shards(config shardctrler.Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups{
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards{
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	//log.Printf("nextconfig %v oldconfig %v", *nextConfig, kv.currentConfig)
	for shardId, gid := range nextConfig.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := kv.db[shardId]; !ok {
			kv.db[shardId] = NewShard()
		}
		if oldgid := kv.currentConfig.Shards[shardId]; oldgid != kv.gid && kv.currentConfig.Num != 0 {
			kv.db[shardId].Status = Pulling
		}
	}
	for shardId, gid := range kv.currentConfig.Shards {
		if gid != kv.gid {
			continue
		}
		if newgid := nextConfig.Shards[shardId]; newgid != kv.gid {
			kv.db[shardId].Status = BePulling
		}
	}

}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandReply {
	//log.Printf("raft[%d]{Group %v} | apply configure nextConfig %v", kv.rf.Me(), kv.gid, *nextConfig)
	if nextConfig.Num == kv.currentConfig.Num + 1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrOutDate, ""}
}

func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	g2s := make(map[int][]int)
	//log.Printf("raft[%d]{Group %v} | lastconfig %v", kv.rf.Me(), kv.gid, kv.lastConfig)
	for gid, _ := range kv.lastConfig.Groups{
		g2s[gid] = make([]int, 0)
	}
	for shardId, gid := range kv.lastConfig.Shards{
		if shard, ok := kv.db[shardId]; ok && shard.Status == status {
			g2s[gid] = append(g2s[gid], shardId)
		}
	}
	//log.Printf("raft[%d]{Group %v} | g2s %v", kv.rf.Me(), kv.gid, g2s)
	return g2s
}	

func (kv *ShardKV) migrationAction(){
	kv.mu.RLock()
	gid2shardIds := kv.getShardIdsByStatus(Pulling)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds{
		//log.Printf("{raft %d}{group %d} start migration task shard %v in group %d when config %v", kv.rf.Me(), kv.gid, shardIds, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers[]string, configNum int, shardIds []int) {
			defer wg.Done()
			pullTaskArgs := ShardOperationRequest{configNum, shardIds}
			for _, server := range servers {
				var pullTaskReply ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardData", &pullTaskArgs, &pullTaskReply) && pullTaskReply.Err == OK{
					kv.mu.Lock()
					kv.mu.Unlock()
					//log.Printf("{raft %d}{group %d} start migration task shard %v in group %d get reply%v", kv.rf.Me(), kv.gid, shardIds, gid, pullTaskReply)
					kv.Execute(NewInsertShardsCommand(&pullTaskReply), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	} 
	kv.mu.RUnlock()
	wg.Wait()
			
} 

func (kv *ShardKV) GetShardData(args *ShardOperationRequest, reply *ShardOperationResponse) {
	if _, isleader := kv.rf.GetState(); !isleader{
		reply.Err = ErrWrongLeader
		return 
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}
	reply.Shards = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.Shards[shardId] = kv.db[shardId].deepCopy()
	}
	reply.LastOperations = make(map[int64]OperationContext)
	for clinetId, operation := range kv.lastOperations{
		reply.LastOperations[clinetId] = operation.deepCopy()
	}
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationResponse) *CommandReply{
	//log.Printf("raft[%d]{Group %v} | apply insert shards", kv.rf.Me(), kv.gid)
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardsInfo.Shards {
			shard := kv.db[shardId] 
			if shard.Status == Pulling {
				for k, v := range shardData{
					shard.KV[k] = v
				}
				shard.Status = GCing
			}else {
				break
			}
			
		}
		for clientId, operationContext := range shardsInfo.LastOperations {
			if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.CommandId < operationContext.CommandId{
				kv.lastOperations[clientId] = operationContext
			}
		}
		return &CommandReply{OK, ""}
	}
	return &CommandReply{ErrOutDate, ""}
}

func (kv *ShardKV) gcAction() {
	kv.mu.RLock()
	gid2shardIds := kv.getShardIdsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		//log.Printf("{raft %d}{group %d} start GC task shard %v in group %d when config %v", kv.rf.Me(), kv.gid, shardIds, gid, kv.currentConfig)
		wg.Add(1)
		go func (servers []string, configNum int, shardIds []int){
			defer wg.Done()
			gcTaskArgs := ShardOperationRequest{configNum, shardIds}
			for _, server := range servers {
				var gcTaskReply ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.DeleteShardsData", &gcTaskArgs, &gcTaskReply) && gcTaskReply.Err == OK {
					//log.Printf("{raft %d}{group %d}  GC task shard %v in group %d get gc reply", kv.rf.Me(), kv.gid, shardIds, gid)
					kv.mu.Lock()
					kv.mu.Unlock()
					//log.Printf("{raft %d}{group %d}  GC task before execute ", kv.rf.Me(), kv.gid)
					kv.Execute(NewDeleteShardsCommand(&gcTaskArgs), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationRequest, reply *ShardOperationResponse) {
	if _, isleader := kv.rf.GetState(); !isleader{
		reply.Err = ErrWrongLeader
		return 
	}
	kv.mu.RLock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	var commandreply CommandReply
	kv.Execute(NewDeleteShardsCommand(args), &commandreply)

	reply.Err = commandreply.Err
}

func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationRequest) *CommandReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardsInfo.ShardIds{
			shard := kv.db[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			}else if shard.Status == BePulling {
				kv.db[shardId] = NewShard()
			}else {
				break
			}
		}
		return &CommandReply{OK, ""}
	}
	return &CommandReply{OK, ""}
}

func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm(){
		kv.Execute(NewEmptyEntryCommand(), &CommandReply{})
	}
}

func (kv *ShardKV) applyEmptyEnty() *CommandReply {
	return &CommandReply{OK, ""}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandArgs{})
	labgob.Register(CommandReply{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationResponse{})
	labgob.Register(ShardOperationRequest{})

	kv := new(ShardKV)
	kv.dead = 0
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.sc = shardctrler.MakeClerk(ctrlers)
	kv.lastApplied = 0
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.lastConfig = shardctrler.DefaultConfig()
	kv.db = make(map[int]*Shard) 
	kv.lastOperations = make(map[int64]OperationContext)
	kv.notifyChans = make(map[int]chan *CommandReply)
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	kv.InstallSnapshot(snapshot)

	go kv.applier()
	go kv.Monitor(kv.configureAction, time.Duration(ConfigureMonitorTimeout) * time.Millisecond)
	go kv.Monitor(kv.migrationAction, time.Duration(MigrationMonitorTimeout) * time.Millisecond)
	go kv.Monitor(kv.gcAction, time.Duration(GCMonitorTimeout) * time.Millisecond)
	go kv.Monitor(kv.checkEntryInCurrentTermAction, time.Duration(EmptyEntryDetectorTimeout) * time.Millisecond)
	log.Printf("raft[%d]{Group %v} start", kv.rf.Me(), kv.gid)
	return kv
}


func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isleader := kv.rf.GetState(); isleader{
			action()
		} 
		time.Sleep(timeout)
	}
}