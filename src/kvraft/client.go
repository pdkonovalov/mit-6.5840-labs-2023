package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const waitResultTimeout = 50 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	cid        int
	seq        int
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
	ck.cid = int(nrand())
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	index := ck.lastLeader
	args := GetArgs{key, ck.cid, ck.seq}
	ck.seq++
	for {
		reply := GetReply{}
		resCh := make(chan bool)
		go func() {
			resCh <- ck.servers[index].Call("KVServer.Get", &args, &reply)
		}()
		ok := false
		select {
		case ok = <-resCh:
		case <-time.NewTimer(waitResultTimeout).C:
		}
		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		index++
		if index == len(ck.servers) {
			index = 0
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	index := ck.lastLeader
	args := PutAppendArgs{key, value, op, ck.cid, ck.seq}
	ck.seq++
	for {
		reply := PutAppendReply{}
		resCh := make(chan bool)
		go func() {
			resCh <- ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		}()
		ok := false
		select {
		case ok = <-resCh:
		case <-time.NewTimer(waitResultTimeout).C:
		}
		if ok && reply.Err != ErrWrongLeader {
			ck.lastLeader = index
			return
		}
		index++
		if index == len(ck.servers) {
			index = 0
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
