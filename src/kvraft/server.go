package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid   int
	Seq   int
	Type  string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	persister *raft.Persister

	db         map[string]string
	lastSeq    map[int]int
	lastGetRes map[int]GetReply
	lastIndex  int

	notify map[int]*sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	kv.mu.Lock()
	kv.checkRaftStateSize()
	if getRes, ok := kv.lastGetRes[args.Cid]; ok && kv.lastSeq[args.Cid] == args.Seq {
		reply.Err = getRes.Err
		reply.Value = getRes.Value
	} else {
		op := Op{args.Cid, args.Seq, "Get", args.Key, ""}
		_, _, isLeader := kv.rf.Start(op)
		if isLeader {
			if _, ok := kv.notify[args.Cid]; !ok {
				kv.notify[args.Cid] = sync.NewCond(&kv.mu)
			}
			kv.notify[args.Cid].Wait()
			if kv.lastSeq[args.Cid] == args.Seq {
				reply.Err = kv.lastGetRes[args.Cid].Err
				reply.Value = kv.lastGetRes[args.Cid].Value
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = ErrWrongLeader
	kv.mu.Lock()
	kv.checkRaftStateSize()
	if lastSeq, ok := kv.lastSeq[args.Cid]; ok && lastSeq == args.Seq {
		reply.Err = OK
	} else {
		op := Op{args.Cid, args.Seq, args.Op, args.Key, args.Value}
		_, _, isLeader := kv.rf.Start(op)
		if isLeader {
			if _, ok := kv.notify[args.Cid]; !ok {
				kv.notify[args.Cid] = sync.NewCond(&kv.mu)
			}
			kv.notify[args.Cid].Wait()
			if kv.lastSeq[args.Cid] == args.Seq {
				reply.Err = OK
			}
		}
	}
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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

	kv.persister = persister

	kv.db = make(map[string]string)
	kv.lastSeq = make(map[int]int)
	kv.lastGetRes = make(map[int]GetReply)
	kv.readSnapshot(persister.ReadSnapshot())

	kv.notify = make(map[int]*sync.Cond)

	go kv.applyChHandler()

	return kv
}

func (kv *KVServer) applyChHandler() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			lastSeq, ok := kv.lastSeq[op.Cid]
			if !ok || op.Seq == lastSeq+1 {
				switch op.Type {
				case "Get":
					if v, haveKey := kv.db[op.Key]; haveKey {
						kv.lastGetRes[op.Cid] = GetReply{OK, v}
					} else {
						kv.lastGetRes[op.Cid] = GetReply{ErrNoKey, ""}
					}
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				kv.lastSeq[op.Cid] = op.Seq
				if _, wait := kv.notify[op.Cid]; wait {
					kv.notify[op.Cid].Broadcast()
				}
			}
			kv.lastIndex = msg.CommandIndex
			kv.checkRaftStateSize()
		} else if msg.SnapshotValid {
			kv.readSnapshot(msg.Snapshot)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(&kv.db) != nil ||
		e.Encode(&kv.lastSeq) != nil ||
		e.Encode(&kv.lastGetRes) != nil ||
		e.Encode(&kv.lastIndex) != nil {
		log.Fatalf("failed to encode KVServer state")
	}
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastSeq map[int]int
	var lastGetRes map[int]GetReply
	var lastIndex int
	if d.Decode(&db) != nil ||
		d.Decode(&lastSeq) != nil ||
		d.Decode(&lastGetRes) != nil ||
		d.Decode(&lastIndex) != nil {
		log.Fatal("failed to decode KVServer state")
	}
	kv.db = db
	kv.lastSeq = lastSeq
	kv.lastGetRes = lastGetRes
	kv.lastIndex = lastIndex
}

func (kv *KVServer) checkRaftStateSize() {
	if kv.maxraftstate != -1 && kv.maxraftstate/2 <= kv.persister.RaftStateSize() {
		snapshot := kv.makeSnapshot()
		kv.rf.Snapshot(kv.lastIndex, snapshot)
	}
}
