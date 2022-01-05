package raft

import (
	raftpb "Distributed_key_value_system/raftrpc"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

type RaftLog struct {
	// 已提交日志的最大位置的索引
	committed 	int64
	// 在本地应用过的日志索引
	lastappliedIndex 	int64
	entries []raftpb.Entry
}


// LastIndex return the last index of the log entries
func (log *RaftLog) LastIndex() int64 {
	logLen := int64(len(log.entries))
	if logLen > 0{
		return log.entries[logLen - 1].GetIndex()
	}else {
		return -1
	}
}

// Term return the term of the entry in the given index
func (log *RaftLog) Term(i int64) (int64, error) {
	if i < int64(len(log.entries)){
		return log.entries[i].GetTerm(),nil
	}else{
		return -1, errors.New("index out of range")
	}

}


func (log * RaftLog) GetLogDataByIndex(i int64)(raftpb.Entry, error){
	if i < int64(len(log.entries)){
		return log.entries[i],nil
	}else{
		return raftpb.Entry{}, errors.New("index out of range")
	}
}

func (log * RaftLog) Append(logEntry  * raftpb.Entry){
	log.entries = append(log.entries, *logEntry)
}