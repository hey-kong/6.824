package shardkv

import (
	"bytes"
	"fmt"

	"6.824/labgob"
	"6.824/shardmaster"
)

// 检测Raft日志大小
func (kv *ShardKV) checkSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	if float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.95 {
		go kv.encodeSnapshot(index)
	}
}

// 快照数据编码
func (kv *ShardKV) encodeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2seq)
	e.Encode(kv.comeInShards)
	e.Encode(kv.toOutShards)
	e.Encode(kv.shards)
	e.Encode(kv.cfg)
	e.Encode(kv.garbages)
	kv.mu.Unlock()
	kv.rf.StartSnapshotOn(index, w.Bytes())
}

// 快照数据解码
func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var myShards map[int]bool
	var garbages map[int]map[int]bool
	var cfg shardmaster.Config
	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil || d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil || d.Decode(&myShards) != nil || d.Decode(&cfg) != nil ||
		d.Decode(&garbages) != nil {
		fmt.Errorf("[decodeSnapshot]: Decode Error!\n")
	} else {
		kv.db, kv.cid2seq, kv.cfg = db, cid2Seq, cfg
		kv.toOutShards, kv.comeInShards, kv.shards, kv.garbages = toOutShards, comeInShards, myShards, garbages
	}
}
