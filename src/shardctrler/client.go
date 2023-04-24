package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

const MoveOp = "Move"
const JoinOp = "Join"
const QueryOp = "Query"
const LeaveOp = "Leave"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	requestId int
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
	// Your code here.
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{}
	// Your code here.
	args.Num = num
	args.ClientId, args.RequestId = ck.clientId, ck.requestId
	args.Op = QueryOp
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && reply.Err == "OK" {
				ck.requestId ++ 
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId, args.RequestId = ck.clientId, ck.requestId
	args.Op = JoinOp
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && reply.Err == "OK" {
				ck.requestId ++ 
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId, args.RequestId = ck.clientId, ck.requestId
	args.Op = LeaveOp
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && reply.Err == "OK" {
				ck.requestId ++ 
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId, args.RequestId = ck.clientId, ck.requestId
	args.Op = MoveOp
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardCtrler.Command", args, &reply)
			if ok && reply.Err == "OK" {
				ck.requestId ++ 
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
