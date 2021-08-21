package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

const RetryInterval = 100 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid        int64
	seq        int
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
	ck.cid = nrand()
	ck.seq = 0
	ck.lastLeader = 0
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	index := ck.lastLeader
	args := GetArgs{Key: key, Cid: ck.cid, Seq: ck.seq}
	for {
		reply := GetReply{}
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	index := ck.lastLeader
	args := PutAppendArgs{key, value, op, ck.cid, ck.seq}
	ck.seq++
	for {
		reply := PutAppendReply{}
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
