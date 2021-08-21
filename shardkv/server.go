package shardkv

import (
	"strconv"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardmaster"
)

type Op struct {
	Type  string
	Key   string
	Value string
	Cid   int64
	Seq   int
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck       *shardmaster.Clerk
	cfg       shardmaster.Config
	persister *raft.Persister
	db        map[string]string
	notify    map[int]chan Op
	cid2seq   map[int64]int

	toOutShards  map[int]map[int]map[string]string "cfg number -> (shard -> db)"
	comeInShards map[int]int                       "shard -> config number"
	shards       map[int]bool                      "record which i-shard can offer service"
	garbages     map[int]map[int]bool              "cfg number -> shards"

	killCh chan bool
}

func equalOp(a Op, b Op) bool {
	return a.Type == b.Type && a.Key == b.Key && a.Cid == b.Cid && a.Seq == b.Seq
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	command := Op{"Get", args.Key, "", nrand(), 0}
	reply.Err, reply.Value = kv.handle(command)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{args.Op, args.Key, args.Value, args.Cid, args.Seq}
	reply.Err, _ = kv.handle(command)
}

func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {
		return
	}
	command := Op{"GC", strconv.Itoa(args.ConfigNum), "", nrand(), args.Shard}
	kv.mu.Unlock()
	reply.Err, _ = kv.handle(command)
	kv.mu.Lock()
}

func (kv *ShardKV) handle(command Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		ch := kv.put(index, true)
		op := kv.notified(ch, index)
		if equalOp(command, op) {
			return OK, op.Value
		}
		if op.Type == ErrWrongGroup {
			return ErrWrongGroup, ""
		}
	}
	return ErrWrongLeader, ""
}

func send(notifyCh chan Op, op Op) {
	select {
	case <-notifyCh:
	default:
	}
	notifyCh <- op
}

func (kv *ShardKV) notified(ch chan Op, index int) Op {
	select {
	case notifyArg, ok := <-ch:
		if ok {
			close(ch)
		}
		kv.mu.Lock()
		delete(kv.notify, index)
		kv.mu.Unlock()
		return notifyArg
	case <-time.After(time.Duration(1000) * time.Millisecond):
		return Op{}
	}
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.cfg.Num {
		return
	}
	reply.Err, reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.DB, reply.Cid2Seq = kv.copyDBAndDedupMap(args.ConfigNum, args.Shard)
}

func (kv *ShardKV) copyDBAndDedupMap(config int, shard int) (map[string]string, map[int64]int) {
	db := make(map[string]string)
	cid2seq := make(map[int64]int)
	for k, v := range kv.toOutShards[config][shard] {
		db[k] = v
	}
	for k, v := range kv.cid2seq {
		cid2seq[k] = v
	}
	return db, cid2seq
}

func (kv *ShardKV) put(idx int, createIfNotExists bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.notify[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		kv.notify[idx] = make(chan Op, 1)
	}
	return kv.notify[idx]
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	select {
	case <-kv.killCh:
	default:
	}
	kv.killCh <- true
}

func (kv *ShardKV) tryPollNewCfg() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) > 0 {
		kv.mu.Unlock()
		return
	}
	next := kv.cfg.Num + 1
	kv.mu.Unlock()
	cfg := kv.mck.Query(next)
	if cfg.Num == next {
		kv.rf.Start(cfg) //sync follower with new cfg
	}
}

func (kv *ShardKV) tryGC() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(shard int, cfg shardmaster.Config) {
				defer wait.Done()
				args := MigrateArgs{shard, cfg.Num}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := MigrateReply{}
					if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						delete(kv.garbages[cfgNum], shard)
						if len(kv.garbages[cfgNum]) == 0 {
							delete(kv.garbages, cfgNum)
						}
						kv.mu.Unlock()
					}
				}
			}(shard, kv.mck.Query(cfgNum))
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) tryPullShard() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) == 0 {
		kv.mu.Unlock()
		return
	}

	var wait sync.WaitGroup
	for shard, idx := range kv.comeInShards {
		wait.Add(1)
		go func(shard int, cfg shardmaster.Config) {
			defer wait.Done()
			args := MigrateArgs{shard, cfg.Num}
			gid := cfg.Shards[shard]
			for _, server := range cfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK {
					kv.rf.Start(reply)
				}

			}
		}(shard, kv.mck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		select {
		case <-kv.killCh:
			return
		default:
			do()
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	if cfg, ok := applyMsg.Command.(shardmaster.Config); ok {
		kv.updateInAndOutDataShard(cfg)
	} else if migrationData, ok := applyMsg.Command.(MigrateReply); ok {
		kv.updateDBWithMigrateData(migrationData)
	} else {
		op := applyMsg.Command.(Op)
		if op.Type == "GC" {
			cfgNum, _ := strconv.Atoi(op.Key)
			kv.gc(cfgNum, op.Seq)
		} else {
			kv.normal(&op)
		}
		if notifyCh := kv.put(applyMsg.CommandIndex, false); notifyCh != nil {
			send(notifyCh, op)
		}
	}
	kv.checkSnapshot(applyMsg.CommandIndex)
}

func (kv *ShardKV) gc(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[cfgNum]; ok {
		delete(kv.toOutShards[cfgNum], shard)
		if len(kv.toOutShards[cfgNum]) == 0 {
			delete(kv.toOutShards, cfgNum)
		}
	}
}

func (kv *ShardKV) updateInAndOutDataShard(cfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Num <= kv.cfg.Num { //only consider newer config
		return
	}

	oldCfg, toOutShard := kv.cfg, kv.shards
	kv.shards, kv.cfg = make(map[int]bool), cfg
	for shard, gid := range cfg.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toOutShard[shard]; ok || oldCfg.Num == 0 {
			kv.shards[shard] = true
			delete(toOutShard, shard)
		} else {
			kv.comeInShards[shard] = oldCfg.Num
		}
	}
	if len(toOutShard) > 0 { // prepare data that needed migration
		kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			outDb := make(map[string]string)
			for k, v := range kv.db {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.db, k)
				}
			}
			kv.toOutShards[oldCfg.Num][shard] = outDb
		}
	}
}

func (kv *ShardKV) updateDBWithMigrateData(migrationData MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrationData.ConfigNum != kv.cfg.Num-1 {
		return
	}

	delete(kv.comeInShards, migrationData.Shard)
	//this check is necessary, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
	if _, ok := kv.shards[migrationData.Shard]; !ok {
		kv.shards[migrationData.Shard] = true
		for k, v := range migrationData.DB {
			kv.db[k] = v
		}
		for k, v := range migrationData.Cid2Seq {
			kv.cid2seq[k] = max(v, kv.cid2seq[k])
		}
		if _, ok := kv.garbages[migrationData.ConfigNum]; !ok {
			kv.garbages[migrationData.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[migrationData.ConfigNum][migrationData.Shard] = true
	}
}

func (kv *ShardKV) normal(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	if _, ok := kv.shards[shard]; !ok {
		op.Type = ErrWrongGroup
	} else {
		maxSeq, found := kv.cid2seq[op.Cid]
		if !found || op.Seq > maxSeq {
			if op.Type == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.db[op.Key] += op.Value
			}
			kv.cid2seq[op.Cid] = op.Seq
		}
		if op.Type == "Get" {
			op.Value = kv.db[op.Key]
		}
	}
	kv.mu.Unlock()
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfg = shardmaster.Config{}

	kv.db = make(map[string]string)
	kv.notify = make(map[int]chan Op)
	kv.cid2seq = make(map[int64]int)

	kv.toOutShards = make(map[int]map[int]map[string]string)
	kv.comeInShards = make(map[int]int)
	kv.shards = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)

	kv.decodeSnapshot(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool, 1)
	go kv.daemon(kv.tryPollNewCfg, 50)
	go kv.daemon(kv.tryPullShard, 80)
	go kv.daemon(kv.tryGC, 100)

	go kv.applyDaemon()
	return kv
}

func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case <-kv.killCh:
			return
		case applyMsg := <-kv.applyCh:
			if !applyMsg.CommandValid {
				kv.decodeSnapshot(applyMsg.SnapShot)
				continue
			}
			kv.apply(applyMsg)
		}
	}
}
