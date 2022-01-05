package raft

import (
	raftpb "Distributed_key_value_system/raftrpc"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)


func TestRaftElection(t *testing.T) {
	setting := Setting{NodeAddress: ":50051", HeartbeatTimeout: 1,ElectionTimeout: 3,PeerNodeAddress:[]string{":50052",":50053"}}
	r1 := Raft{}
	r1.NewRaft(&setting)


	setting2 := Setting{NodeAddress: ":50052", HeartbeatTimeout: 1,ElectionTimeout: 5,PeerNodeAddress:[]string{":50051",":50053"}}
	r2 := Raft{}
	r2.NewRaft(&setting2)

	setting3 := Setting{NodeAddress: ":50053", HeartbeatTimeout: 1,ElectionTimeout: 10,PeerNodeAddress:[]string{":50051",":50052"}}
	r3 := Raft{}
	r3.NewRaft(&setting3)


	for i:= 0;i < 10;i++{
		r1.LogicalTimerChange()
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()
	}


	assert.Equal(t, r1.State, Leader)
	assert.Equal(t, r2.leaderAddress, ":50051")
	assert.Equal(t, r3.leaderAddress, ":50051")

	// leader怠机，会重新选举
	r1.StopRaft()

	for i:= 0;i < 10;i++{
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()
	}

	assert.Equal(t, r2.State, Leader)
	assert.Equal(t, r3.leaderAddress, ":50052")
	r2.StopRaft()
	r3.StopRaft()
}

// 测试没有错误出现时的日志复制
func TestRaftAppend(t *testing.T) {
	setting := Setting{NodeAddress: ":50051", HeartbeatTimeout: 1,ElectionTimeout: 3,PeerNodeAddress:[]string{":50052",":50053"}}
	r1 := Raft{}
	r1.NewRaft(&setting)


	setting2 := Setting{NodeAddress: ":50052", HeartbeatTimeout: 1,ElectionTimeout: 6,PeerNodeAddress:[]string{":50051",":50053"}}
	r2 := Raft{}
	r2.NewRaft(&setting2)

	setting3 := Setting{NodeAddress: ":50053", HeartbeatTimeout: 1,ElectionTimeout: 10,PeerNodeAddress:[]string{":50051",":50052"}}
	r3 := Raft{}
	r3.NewRaft(&setting3)



	kventry := KVentry{}
	kventry.KVOperation = Put
	kventry.Key = []byte("hello")
	kventry.Value = []byte("world")
	kventryByte,_ := json.Marshal(kventry)


	kventry2 := KVentry{}
	kventry2.KVOperation = Put
	kventry2.Key = []byte("test2")
	kventry2.Value = []byte("test2")
	kventryByte2,_ := json.Marshal(kventry2)

	kventry3 := KVentry{}
	kventry3.KVOperation = Put
	kventry3.Key = []byte("test3")
	kventry3.Value = []byte("test3")
	kventryByte3,_ := json.Marshal(kventry3)


	for i:= 0;i < 20;i++{
		r1.LogicalTimerChange()
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()
		if i == 10{
			assert.Equal(t, r1.State,Leader)
			r1.Append(kventryByte)
		}

		if i == 13{
			r1.Append(kventryByte2)
		}

		if i == 15{
			success := r1.Append(kventryByte3)
			assert.Equal(t, r1.Log.committed, int64(2))
			assert.Equal(t, success, true)
		}

	}

	assert.Equal(t, r2.Log.entries[1].GetTerm(),int64(1))
	assert.Equal(t, r2.Log.entries[1].GetIndex(),int64(1))
	assert.Equal(t, r2.Log.entries[1].GetData(),kventryByte2)

	assert.Equal(t, r2.Log.entries[2].GetTerm(),int64(1))
	assert.Equal(t, r2.Log.entries[2].GetIndex(),int64(2))
	assert.Equal(t, r2.Log.entries[2].GetData(),kventryByte3)


	assert.Equal(t, r3.Log.entries[2].GetTerm(),int64(1))
	assert.Equal(t, r3.Log.entries[2].GetIndex(),int64(2))
	assert.Equal(t, r3.Log.entries[2].GetData(),kventryByte3)



	r1.StopRaft()
	r2.StopRaft()
	r3.StopRaft()

}

// 测试日志不一致的情况
func TestRaftAppend2(t *testing.T) {
	setting := Setting{NodeAddress: ":50051", HeartbeatTimeout: 1,ElectionTimeout: 3,PeerNodeAddress:[]string{":50052",":50053"}}
	r1 := Raft{}
	r1.NewRaft(&setting)


	setting2 := Setting{NodeAddress: ":50052", HeartbeatTimeout: 1,ElectionTimeout: 6,PeerNodeAddress:[]string{":50051",":50053"}}
	r2 := Raft{}
	r2.NewRaft(&setting2)

	setting3 := Setting{NodeAddress: ":50053", HeartbeatTimeout: 1,ElectionTimeout: 10,PeerNodeAddress:[]string{":50051",":50052"}}
	r3 := Raft{}
	r3.NewRaft(&setting3)



	kventry := KVentry{}
	kventry.KVOperation = Put
	kventry.Key = []byte("hello")
	kventry.Value = []byte("world")
	kventryByte,_ := json.Marshal(kventry)


	kventry2 := KVentry{}
	kventry2.KVOperation = Put
	kventry2.Key = []byte("test2")
	kventry2.Value = []byte("test2")
	kventryByte2,_ := json.Marshal(kventry2)

	kventry3 := KVentry{}
	kventry3.KVOperation = Put
	kventry3.Key = []byte("test3")
	kventry3.Value = []byte("test3")
	kventryByte3,_ := json.Marshal(kventry3)

	kventry4 := KVentry{}
	kventry4.KVOperation = Put
	kventry4.Key = []byte("test4")
	kventry4.Value = []byte("test4")
	kventryByte4,_ := json.Marshal(kventry4)

	kventry5 := KVentry{}
	kventry5.KVOperation = Put
	kventry5.Key = []byte("test5")
	kventry5.Value = []byte("test5")
	kventryByte5,_ := json.Marshal(kventry5)


	for i:= 0;i < 20;i++{
		r1.LogicalTimerChange()
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()
		if i == 10{
			r1.Append(kventryByte)
		}

		if i == 13{
			r1.Append(kventryByte2)
		}

		if i == 15{
			r1.Append(kventryByte3)
		}

		// 强制修改日志使得日志不一致
		if i == 16{
			//日志的term不一致
			r3.Log.entries[2].Term = 3
			r3.Log.entries[2].Data = []byte("hhhhh")

			//日志缺少
			r2.Log.entries = r2.Log.entries[:1]
			log.Println(len(r2.Log.entries))
		}


		if i == 18{
			r1.Append(kventryByte4)
		}

	}

	assert.Equal(t, r2.Log.entries[2].GetTerm(),int64(1))
	assert.Equal(t, r2.Log.entries[2].GetIndex(),int64(2))
	assert.Equal(t, r2.Log.entries[2].GetData(),kventryByte3)

	assert.Equal(t, r2.Log.entries[3].GetTerm(),int64(1))
	assert.Equal(t, r2.Log.entries[3].GetIndex(),int64(3))
	assert.Equal(t, r2.Log.entries[3].GetData(),kventryByte4)
	assert.Equal(t, len(r2.Log.entries), 4)


	assert.Equal(t, r3.Log.entries[2].GetTerm(),int64(1))
	assert.Equal(t, r3.Log.entries[2].GetIndex(),int64(2))
	assert.Equal(t, r3.Log.entries[2].GetData(),kventryByte3)

	assert.Equal(t, r3.Log.entries[3].GetTerm(),int64(1))
	assert.Equal(t, r3.Log.entries[3].GetIndex(),int64(3))
	assert.Equal(t, r3.Log.entries[3].GetData(),kventryByte4)
	assert.Equal(t, len(r3.Log.entries), 4)

	//change leader and term
	r1.BecomeFollower(1)
	r2.BecomeCandidate()

	for i:= 0;i < 10;i++{
		r1.LogicalTimerChange()
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()

		if i == 3{
			// log比leader长的情况
			assert.Equal(t, len(r2.Log.entries), 4)
			r3.Log.entries = append(r3.Log.entries, raftpb.Entry{})
			r3.Log.entries = append(r3.Log.entries, raftpb.Entry{})
			assert.Equal(t, len(r3.Log.entries), 6)

			// log变短且不一致
			r1.Log.entries = r1.Log.entries[:3]
			assert.Equal(t, len(r1.Log.entries), 3)
			r1.Log.entries[2].Term = 3
		}


		if i == 5{
			r2.Append(kventryByte5)
		}
	}

	assert.Equal(t, r2.Log.entries[4].GetTerm(),int64(2))
	assert.Equal(t, r2.Log.entries[4].GetIndex(),int64(4))
	assert.Equal(t, r2.Log.entries[4].GetData(),kventryByte5)
	assert.Equal(t, len(r2.Log.entries), 5)

	assert.Equal(t, r1.Log.entries[2].GetTerm(),int64(1))
	assert.Equal(t, r1.Log.entries[2].GetIndex(),int64(2))
	assert.Equal(t, r1.Log.entries[2].GetData(),kventryByte3)


	assert.Equal(t, r1.Log.entries[4].GetTerm(),int64(2))
	assert.Equal(t, r1.Log.entries[4].GetIndex(),int64(4))
	assert.Equal(t, r1.Log.entries[4].GetData(),kventryByte5)
	assert.Equal(t, len(r1.Log.entries), 5)

	assert.Equal(t, r3.Log.entries[4].GetTerm(),int64(2))
	assert.Equal(t, r3.Log.entries[4].GetIndex(),int64(4))
	assert.Equal(t, r3.Log.entries[4].GetData(),kventryByte5)
	assert.Equal(t, len(r3.Log.entries), 5)

	r1.StopRaft()
	r2.StopRaft()
	r3.StopRaft()

}

// raft paper 5.4.1 Election restriction
// 只有日志更加新的才能成为leader
func TestRaftElectionRestriction(t *testing.T) {
	setting := Setting{NodeAddress: ":50051", HeartbeatTimeout: 1,ElectionTimeout: 3,PeerNodeAddress:[]string{":50052",":50053"}}
	r1 := Raft{}
	r1.NewRaft(&setting)


	setting2 := Setting{NodeAddress: ":50052", HeartbeatTimeout: 1,ElectionTimeout: 4,PeerNodeAddress:[]string{":50051",":50053"}}
	r2 := Raft{}
	r2.NewRaft(&setting2)

	setting3 := Setting{NodeAddress: ":50053", HeartbeatTimeout: 1,ElectionTimeout: 5,PeerNodeAddress:[]string{":50051",":50052"}}
	r3 := Raft{}
	r3.NewRaft(&setting3)

	//term相同，日志较长的成为leader
	r1.Log.entries = append(r1.Log.entries, raftpb.Entry{Term: 0, Index: 0})


	r2.Log.entries = append(r2.Log.entries, raftpb.Entry{Term: 0, Index: 0})
	r2.Log.entries = append(r2.Log.entries, raftpb.Entry{Term: 0, Index: 1})

	r3.Log.entries = append(r3.Log.entries, raftpb.Entry{Term: 0, Index: 0})
	r3.Log.entries = append(r3.Log.entries, raftpb.Entry{Term: 0, Index: 1})

	r1.BecomeCandidate()
	r2.BecomeCandidate()


	for i:= 0;i < 5;i ++{
		r1.LogicalTimerChange()
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()
	}

	assert.Equal(t, r1.State, Follower)
	assert.Equal(t, r2.State, Leader)

	// term较新的成为leader，如果candidate的日志比较旧，拒绝投票
	r1.Log.entries[0].Term = 1
	r3.Log.entries[1].Term = 1

	r2.BecomeCandidate()
	r1.BecomeCandidate()


	for i:= 0;i < 5;i ++{
		r1.LogicalTimerChange()
		r2.LogicalTimerChange()
		r3.LogicalTimerChange()
	}

	assert.Equal(t, r1.State, Leader)
	assert.Equal(t, r2.State, Follower)

	r1.StopRaft()
	r2.StopRaft()
	r3.StopRaft()
}