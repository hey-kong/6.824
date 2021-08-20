package shardmaster

import (
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config        // indexed by config num
	notify  map[int]chan Op // 每条log对于一个channel，在server上先写log再reply
	cid2seq map[int64]int   // 检测请求是否重复

	shutdown chan struct{} // shutdown chan
}

type Op struct {
	// Your data here.
	Type string
	Args interface{}
	Cid  int64
	Seq  int
}

func equalOp(a Op, b Op) bool {
	return a.Type == b.Type && a.Seq == b.Seq && a.Cid == b.Cid
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	command := Op{"Join", *args, args.Cid, args.Seq}
	reply.WrongLeader = sm.handle(command)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	command := Op{"Leave", *args, args.Cid, args.Seq}
	reply.WrongLeader = sm.handle(command)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	command := Op{"Move", *args, args.Cid, args.Seq}
	reply.WrongLeader = sm.handle(command)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	reply.WrongLeader = true
	command := Op{"Query", *args, nrand(), -1}
	reply.WrongLeader = sm.handle(command)
	if !reply.WrongLeader {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}
}

func (sm *ShardMaster) handle(command Op) bool {
	wrongLeader := true
	index, _, isLeader := sm.rf.Start(command)
	if !isLeader {
		return wrongLeader
	}
	ch := sm.getCh(index, true)
	op := sm.notified(ch, index)
	if equalOp(op, command) {
		wrongLeader = false
	}
	return wrongLeader
}

func (sm *ShardMaster) getCh(idx int, createIfNotExists bool) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.notify[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		sm.notify[idx] = make(chan Op, 1)
	}
	return sm.notify[idx]
}

func send(notifyCh chan Op, op Op) {
	notifyCh <- op
}

func (sm *ShardMaster) notified(ch chan Op, index int) Op {
	select {
	case notifyArg := <-ch:
		close(ch)
		sm.mu.Lock()
		delete(sm.notify, index)
		sm.mu.Unlock()
		return notifyArg
	case <-time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) updateConfig(op string, args interface{}) {
	cfg := sm.copyLastConfig()

	if op == "Move" {
		moveArgs := args.(MoveArgs)
		if _, exists := cfg.Groups[moveArgs.GID]; exists {
			cfg.Shards[moveArgs.Shard] = moveArgs.GID
		} else {
			return
		}
	} else if op == "Join" {
		joinArgs := args.(JoinArgs)
		for gid, servers := range joinArgs.Servers {
			cfg.Groups[gid] = servers
			sm.rebalance(&cfg, op, gid)
		}
	} else if op == "Leave" {
		leaveArgs := args.(LeaveArgs)
		for _, gid := range leaveArgs.GIDs {
			delete(cfg.Groups, gid)
			sm.rebalance(&cfg, op, gid)
		}
	} else if op == "Query" {
		// do nothing
	} else {
		log.Fatal("invalid area ", op)
	}
	sm.configs = append(sm.configs, cfg)
}

func (sm *ShardMaster) copyLastConfig() Config {
	lastCfg := sm.configs[len(sm.configs)-1]
	newCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: make(map[int][]string)}
	for gid, servers := range lastCfg.Groups {
		newCfg.Groups[gid] = append([]string{}, servers...)
	}
	return newCfg
}

func (sm *ShardMaster) rebalance(cfg *Config, request string, gid int) {
	groupShards := sm.genGroupShards(cfg) // gid -> shards

	switch request {
	case "Join":
		shardCnt := NShards / len(cfg.Groups)
		for i := 0; i < shardCnt; i++ {
			tmp := sm.getGidOfMaxShardNum(groupShards)
			cfg.Shards[groupShards[tmp][0]] = gid
			groupShards[tmp] = groupShards[tmp][1:]
		}
	case "Leave":
		shards, exist := groupShards[gid]
		if !exist {
			return
		}
		delete(groupShards, gid)
		if len(cfg.Groups) == 0 {
			// remove all groups
			cfg.Shards = [NShards]int{}
			return
		}
		for _, shard := range shards {
			tmp := sm.getGidOfMinShardNum(groupShards)
			cfg.Shards[shard] = tmp
			groupShards[tmp] = append(groupShards[tmp], shard)
		}
	}
}

func (sm *ShardMaster) genGroupShards(cfg *Config) map[int][]int {
	groupShards := map[int][]int{}
	for k, _ := range cfg.Groups {
		groupShards[k] = []int{}
	}
	for k, v := range cfg.Shards {
		groupShards[v] = append(groupShards[v], k)
	}
	return groupShards
}

func (sm *ShardMaster) getGidOfMaxShardNum(groupShards map[int][]int) int {
	max := -1
	gid := 0
	for k, v := range groupShards {
		if max < len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}

func (sm *ShardMaster) getGidOfMinShardNum(groupShards map[int][]int) int {
	min := 1<<31 - 1
	gid := 0
	for k, v := range groupShards {
		if min > len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.notify = make(map[int]chan Op)
	sm.cid2seq = make(map[int64]int)
	sm.shutdown = make(chan struct{})

	go sm.applyDaemon()

	return sm
}

func (sm *ShardMaster) applyDaemon() {
	for {
		select {
		case <-sm.shutdown:
			return
		case applyMsg := <-sm.applyCh:
			if !applyMsg.CommandValid {
				continue
			}
			op := applyMsg.Command.(Op)
			sm.mu.Lock()
			maxSeq, found := sm.cid2seq[op.Cid]
			if op.Seq >= 0 && (!found || op.Seq > maxSeq) {
				sm.updateConfig(op.Type, op.Args)
				sm.cid2seq[op.Cid] = op.Seq
			}
			sm.mu.Unlock()
			if notifyCh := sm.getCh(applyMsg.CommandIndex, false); notifyCh != nil {
				send(notifyCh, op)
			}
		}
	}
}
