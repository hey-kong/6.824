package shardmaster

import (
	"log"
	"sync"

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

	configs   []Config              // indexed by config num
	duplicate map[int64]int         // 检测请求是否重复
	notify    map[int]chan struct{} // 每条log对于一个channel，在server上先写log再reply

	shutdown chan struct{} // shutdown chan
}

type Op struct {
	// Your data here.
	Type string
	Args interface{}
	Cid  int64
	Seq  int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	// 防止重复处理过去的请求
	sm.mu.Lock()
	if seq, ok := sm.duplicate[args.Cid]; ok && seq == args.Seq {
		sm.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	command := Op{
		Type: "Join",
		Args: *args,
		Cid:  args.Cid,
		Seq:  args.Seq,
	}

	// 将command作为log entry写入日志中
	index, startTerm, _ := sm.rf.Start(command)
	notify := make(chan struct{})
	sm.notify[index] = notify
	sm.mu.Unlock()

	// 等待被apply
	select {
	case <-sm.shutdown:
		return
	case <-notify:
		curTerm, isLeader := sm.rf.GetState()
		// leader发生了变化
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = ErrNotLeader
			return
		}

		sm.mu.Lock()
		reply.WrongLeader = false
		reply.Err = OK
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	// 防止重复处理过去的请求
	sm.mu.Lock()
	if seq, ok := sm.duplicate[args.Cid]; ok && seq == args.Seq {
		sm.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	command := Op{
		Type: "Leave",
		Args: *args,
		Cid:  args.Cid,
		Seq:  args.Seq,
	}

	// 将command作为log entry写入日志中
	index, startTerm, _ := sm.rf.Start(command)
	notify := make(chan struct{})
	sm.notify[index] = notify
	sm.mu.Unlock()

	// 等待被apply
	select {
	case <-sm.shutdown:
		return
	case <-notify:
		curTerm, isLeader := sm.rf.GetState()
		// leader发生了变化
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = ErrNotLeader
			return
		}

		sm.mu.Lock()
		reply.WrongLeader = false
		reply.Err = OK
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	// 防止重复处理过去的请求
	sm.mu.Lock()
	if seq, ok := sm.duplicate[args.Cid]; ok && seq == args.Seq {
		sm.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	command := Op{
		Type: "Move",
		Args: *args,
		Cid:  args.Cid,
		Seq:  args.Seq,
	}

	// 将command作为log entry写入日志中
	index, startTerm, _ := sm.rf.Start(command)
	notify := make(chan struct{})
	sm.notify[index] = notify
	sm.mu.Unlock()

	// 等待被apply
	select {
	case <-sm.shutdown:
		return
	case <-notify:
		curTerm, isLeader := sm.rf.GetState()
		// leader发生了变化
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = ErrNotLeader
			return
		}

		sm.mu.Lock()
		reply.WrongLeader = false
		reply.Err = OK
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrNotLeader
		return
	}

	sm.mu.Lock()
	command := Op{
		Type: "Query",
		Args: *args,
	}

	// 将command作为log entry写入日志中
	index, startTerm, _ := sm.rf.Start(command)
	notify := make(chan struct{})
	sm.notify[index] = notify
	sm.mu.Unlock()

	// 等待被apply
	select {
	case <-sm.shutdown:
		return
	case <-notify:
		curTerm, isLeader := sm.rf.GetState()
		// leader发生了变化
		if !isLeader || startTerm != curTerm {
			reply.WrongLeader = true
			reply.Err = ErrNotLeader
			return
		}

		sm.mu.Lock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
		reply.WrongLeader = false
		reply.Err = OK
		sm.mu.Unlock()
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
	sm.notify = make(map[int]chan struct{})
	sm.duplicate = make(map[int64]int)

	sm.shutdown = make(chan struct{})

	go sm.applyDaemon()

	return sm
}

// 后台对日志进行apply
func (sm *ShardMaster) applyDaemon() {
	for {
		select {
		case <-sm.shutdown:
			return
		case applyMsg, ok := <-sm.applyCh:
			if !ok || applyMsg.Command == nil || !applyMsg.CommandValid {
				continue
			}

			command := applyMsg.Command.(Op)
			sm.mu.Lock()
			if seq, ok := sm.duplicate[command.Cid]; !ok || command.Seq > seq {
				sm.updateConfig(command.Type, command.Args)
				sm.duplicate[command.Cid] = command.Seq
			}

			// apply后需要通知当前节点
			if notify, ok := sm.notify[applyMsg.CommandIndex]; ok && notify != nil {
				close(notify)
				delete(sm.notify, applyMsg.CommandIndex)
			}
			sm.mu.Unlock()
		}
	}
}
