package raft


type KVOperations int

const (
	_ KVOperations = iota
	Put
	Get
	Del
)


type KVentry struct{
	KVOperation KVOperations `json:"kv_operation"`
	Value []byte `json:"value"`
	Key   []byte `json:"key"`
}