package raftkv

import (
	"log"
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Get", "Put" or "Append"
	Cid   int64  // client ID
	Seq   int    // request sequence number
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string     // 保存键值对
	notify    map[int]chan struct{} // 每条log对于一个channel，在server上先写log再reply
	cid2seq   map[int64]int         // 检测请求是否重复
	persister *raft.Persister

	shutdown chan struct{} // shutdown chan
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 如果不是leader，直接返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	kv.mu.Lock()
	command := Op{
		Key: args.Key,
		Op:  "Get",
		Cid: args.Cid,
		Seq: args.Seq,
	}

	// 将command作为log entry写入日志中
	index, startTerm, _ := kv.rf.Start(command)
	notify := make(chan struct{})
	kv.notify[index] = notify
	kv.mu.Unlock()

	// 等待被apply
	select {
	case <-kv.shutdown:
		DPrintf("Peer[%d]: shutdown - Get", kv.me)
		return
	case <-notify:
		curTerm, isLeader := kv.rf.GetState()
		// leader发生了变化
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = ErrNotLeader
			return
		}

		kv.mu.Lock()
		if value, ok := kv.db[args.Key]; ok {
			reply.WrongLeader = false
			reply.Value = value
			reply.Err = OK
			DPrintf("Peer[%d]: Get request for Key[%q] Value:%s", kv.me, args.Key, value)
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	kv.mu.Lock()
	command := Op{
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		Cid:   args.Cid,
		Seq:   args.Seq,
	}

	// 将command作为log entry写入日志中
	index, startTerm, _ := kv.rf.Start(command)
	notify := make(chan struct{})
	kv.notify[index] = notify
	kv.mu.Unlock()

	// 等待被apply
	select {
	case <-kv.shutdown:
		DPrintf("Peer[%d]: shutdown - Get", kv.me)
		return
	case <-notify:
		curTerm, isLeader := kv.rf.GetState()
		// leader发生了变化
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = ErrNotLeader
			return
		}

		kv.mu.Lock()
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("Peer[%d]: Put/Append request for Key[%q] Value[%q]", kv.me, args.Key, args.Value)
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.notify = make(map[int]chan struct{})
	kv.cid2seq = make(map[int64]int)
	kv.persister = persister

	kv.shutdown = make(chan struct{})

	kv.decodeSnapshot(kv.persister.ReadSnapshot())

	go kv.applyDaemon()

	return kv
}

// 后台对日志进行apply
func (kv *KVServer) applyDaemon() {
	for {
		select {
		case <-kv.shutdown:
			DPrintf("Peer[%d]: shutdown - applyDaemon", kv.me)
			return
		case applyMsg, ok := <-kv.applyCh:
			if !applyMsg.CommandValid {
				snapshot := applyMsg.Command.([]byte)
				kv.decodeSnapshot(snapshot)
				continue
			}
			if ok && applyMsg.Command != nil && applyMsg.CommandValid {
				command := applyMsg.Command.(Op)
				kv.mu.Lock()
				if seq, ok := kv.cid2seq[command.Cid]; !ok || command.Seq > seq {
					kv.cid2seq[command.Cid] = command.Seq
					switch command.Op {
					case "Get":
						// do nothing
					case "Put":
						kv.db[command.Key] = command.Value
					case "Append":
						kv.db[command.Key] += command.Value
					default:
						DPrintf("Peer[%d]: receive unknown command: %s",
							kv.me, command.Op)
					}
				}

				// apply后需要通知当前节点
				if notify, ok := kv.notify[applyMsg.CommandIndex]; ok && notify != nil {
					close(notify)
					delete(kv.notify, applyMsg.CommandIndex)
				}
				kv.checkSnapshot(applyMsg.CommandIndex)
				kv.mu.Unlock()
			}
		}
	}
}
