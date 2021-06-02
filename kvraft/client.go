package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

const RetryInterval = 600 * time.Millisecond

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
	// You will have to modify this function.
	ck.seq++
	args := GetArgs{Key: key, Cid: ck.cid, Seq: ck.seq}

	for {
		reply := GetReply{}
		doneCh := make(chan bool)
		go func() {
			// 发送RPC，等待reply
			ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)
			doneCh <- ok
		}()

		select {
		case <-time.After(RetryInterval):
			DPrintf("clerk(%d) retry Get after timeout\n", ck.cid)
			continue
		case ok := <-doneCh:
			// 收到响应后，并且是leader返回的，那么说明这个命令已经执行了
			if ok && !reply.WrongLeader {
				if reply.Err == OK {
					return reply.Value
				} else {
					return ""
				}
			}
		}

		// 不是leader返回的，换一个server重发
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
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
	// You will have to modify this function.
	ck.seq++
	args := PutAppendArgs{Key: key, Value: value, Op: op, Cid: ck.cid, Seq: ck.seq}

	for {
		reply := PutAppendReply{}
		doneCh := make(chan bool)
		go func() {
			// 发送RPC，等待reply
			ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)
			doneCh <- ok
		}()

		select {
		case <-time.After(RetryInterval):
			DPrintf("clerk(%d) retry PutAppend after timeout\n", ck.cid)
			continue
		case ok := <-doneCh:
			// 收到响应后，并且是leader返回的，那么说明这个命令已经执行了
			if ok && !reply.WrongLeader {
				return
			}
		}

		// 不是leader返回的，换一个server重发
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
