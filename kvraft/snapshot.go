package raftkv

import (
	"bytes"

	"6.824/labgob"
)

// 检测Raft日志大小
func (kv *KVServer) checkSnapshot(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	if float64(kv.persister.RaftStateSize())/float64(kv.maxraftstate) > 0.95 {
		go kv.rf.StartSnapshotOn(index, kv.encodeSnapshot())
	}
}

// 快照数据编码
func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.cid2seq)
	return w.Bytes()
}

// 快照数据解码
func (kv *KVServer) decodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2seq map[int64]int
	d.Decode(&db)
	d.Decode(&cid2seq)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.db = db
	kv.cid2seq = cid2seq
	return
}
