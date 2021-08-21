package raftkv

import (
	"strconv"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	OpType string "operation type(eg. put/append)"
	Key    string
	Value  string
	Cid    int64
	SeqNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string // 保存键值对
	notify    map[int]chan Op   // 每条log对于一个channel，在server上先写log再reply
	cid2seq   map[int64]int     // 检测请求是否重复
	persister *raft.Persister

	shutdown chan struct{} // shutdown chan
}

func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value && a.OpType == b.OpType && a.SeqNum == b.SeqNum && a.Cid == b.Cid
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//from hint: A simple solution is to enter every Get() (as well as each Put() and Append()) in the Raft log.
	originOp := Op{"Get", args.Key, strconv.FormatInt(nrand(), 10), 0, 0}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	ch := kv.putIfAbsent(index)
	op := notified(ch)
	if equalOp(op, originOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originOp := Op{args.Op, args.Key, args.Value, args.Cid, args.Seq}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	ch := kv.putIfAbsent(index)
	op := notified(ch)
	if equalOp(originOp, op) {
		reply.WrongLeader = false
	}
}

func send(notifyCh chan Op, op Op) {
	select {
	case <-notifyCh:
	default:
	}
	notifyCh <- op
}

func notified(ch chan Op) Op {
	select {
	case notifyArg := <-ch:
		return notifyArg
	case <-time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

func (kv *KVServer) putIfAbsent(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.notify[idx]; !ok {
		kv.notify[idx] = make(chan Op, 1)
	}
	return kv.notify[idx]
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
	kv.notify = make(map[int]chan Op)
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
			return
		case applyMsg := <-kv.applyCh:
			if !applyMsg.CommandValid {
				kv.decodeSnapshot(applyMsg.SnapShot)
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			maxSeq, found := kv.cid2seq[op.Cid]
			if !found || op.SeqNum > maxSeq {
				switch op.OpType {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.cid2seq[op.Cid] = op.SeqNum
			}
			kv.mu.Unlock()
			notifyCh := kv.putIfAbsent(applyMsg.CommandIndex)
			kv.checkSnapshot(applyMsg.CommandIndex)
			send(notifyCh, op)
		}
	}
}
