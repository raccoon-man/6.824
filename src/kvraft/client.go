package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
//import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{}
	args.ClientId, args.RequestId = ck.clientId, ck.requestId
	args.Key = key
	//log.Printf("client %d send a Get", ck.clientId)
	for {
		reply := GetReply{}
		if !ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout  {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.requestId ++ 
		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.ClientId, args.RequestId = ck.clientId, ck.requestId
	args.Key, args.Value, args.Op = key, value, op
	for {
		reply := PutAppendReply{}
		//log.Printf("client %d send a PutAppend", ck.clientId)
		if !ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout  {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.requestId ++ 
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
