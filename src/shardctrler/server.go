package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "time"
import "log"
import "sort"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg


	lastApplied int
	// Your data here.
	g2shard map[int][]int
	lastOperations map[int64]int
	msgCh map[int]chan CommandReply

	configs []Config // indexed by config num
}



type Op struct {
	// Your data here.
	Command OperationOp
	ClientId int64
	RequestId int
	Servers map[int][]string
	GIDs []int
	Shard int
	GID int
	Num int
}

func DefaultConfig() Config {
	config := Config{}
	return config
}

func Group2Shards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups{
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards{
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func (sc *ShardCtrler) isDuplicateRequest(clinetId int64, requestId int) bool  {
	appliedRequestId, ok := sc.lastOperations[clinetId]
	if ok == false || requestId > appliedRequestId {
		return false 
	}
	return true
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	op := Op{
		Command : args.Op,
		ClientId : args.ClientId,
		RequestId : args.RequestId,
		Servers : args.Servers,
		GIDs : args.GIDs,
		Shard : args.Shard,
		GID : args.GID,
		Num : args.Num,
	}
	index, _, isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf("ctrler accept a command %v", args)
	sc.mu.Lock()
	if _, ok := sc.msgCh[index]; !ok {
		sc.msgCh[index] = make(chan CommandReply, 1)
	}
	ch := sc.msgCh[index]
	sc.mu.Unlock()

	select{
	case notify := <-ch:
		reply.Config, reply.Err = notify.Config, notify.Err
	case <- time.After(500*time.Millisecond):
		reply.Err = ErrTimeout
	}

	sc.mu.Lock()
	delete(sc.msgCh, index)
	sc.mu.Unlock()
}


func (sc *ShardCtrler) applier(){
	for {
		select{
		case msg := <- sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex
				var response CommandReply
				op := msg.Command.(Op)
				log.Printf("ctrler apply a command %v", op)
				switch op.Command {
				case QueryOp:
					response.Config, response.Err = sc.Query(op.Num)
				case JoinOp:
					response.Err = sc.Join(op.Servers)
				case MoveOp:
					response.Err = sc.Move(op.Shard, op.GID)
				case LeaveOp:
					response.Err = sc.Leave(op.GIDs)
				}
				sc.lastOperations[op.ClientId] = op.RequestId

				if ch, ok := sc.msgCh[msg.CommandIndex]; ok{
					ch <- response
				}
				sc.mu.Unlock()
			}
		}
	}
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func (sc *ShardCtrler) Join(groups map[int][]string) Err {
	lastConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num : len(sc.configs),
		Shards : lastConfig.Shards,
		Groups : deepCopy(lastConfig.Groups),
	}
	for gid, servers := range groups{
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	g2s := Group2Shards(newConfig)

	for {
		s, t := getMaxNumberShardByGid(g2s), getMinNumberShardByGid(g2s)
		if s != 0 && len(g2s[s]) - len(g2s[t]) <= 1{
			break
		}
		g2s[t] = append(g2s[t], g2s[s][0])
		g2s[s] = g2s[s][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards{
			newShards[shard] = gid
 		} 
	}
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) Leave(GIDs []int) Err {
	// Your code here.
	lastConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num : len(sc.configs),
		Shards : lastConfig.Shards,
		Groups : deepCopy(lastConfig.Groups),
	}
	g2s := Group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok{
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards{
			t := getMinNumberShardByGid(g2s)
			g2s[t] = append(g2s[t], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards{
				newShards[shard] = gid
			 } 
		}
	}
	newConfig.Shards = newShards

	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) Move(shard int, gid int) Err {
	// Your code here.
	lastConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{
		Num : len(sc.configs),
		Shards : lastConfig.Shards,
		Groups : deepCopy(lastConfig.Groups),
	}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
	return OK
}

func (sc *ShardCtrler) Query(num int) (Config, Err) {
	// Your code here.
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs) - 1], OK
	}
	return sc.configs[num], OK
}


func getMinNumberShardByGid(g2s map[int][]int) int {
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	min, index := NShards+1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			min = len(g2s[gid])
			index = gid
		}
	}
	return index
}

func getMaxNumberShardByGid(g2s map[int][]int) int {
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	max, index := -1, -1
	for _, gid := range gids {
		if len(g2s[gid]) > max {
			max = len(g2s[gid])
			index = gid
		}
	}
	return index
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()

	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastOperations = make(map[int64]int)
	sc.msgCh = make(map[int]chan CommandReply)
	go sc.applier()
	// Your code here.

	return sc
}
