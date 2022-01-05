package kv_server

import (
	"Distributed_key_value_system/kv_client"
	pb "Distributed_key_value_system/kvrpc"
	"Distributed_key_value_system/raft"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestServer(t *testing.T) {

	// 启动3个kvsever节点
	s1, kvserver1 := NewServer(&raft.Setting{NodeAddress: ":50051", HeartbeatTimeout: 1,ElectionTimeout: 3,PeerNodeAddress:[]string{":50052",":50053"}},"mydb1",":50041")

	s2, kvserver2 := NewServer(&raft.Setting{NodeAddress: ":50052", HeartbeatTimeout: 1,ElectionTimeout: 6,PeerNodeAddress:[]string{":50051",":50053"}},"mydb2",":50042")

	s3, kvserver3 := NewServer(&raft.Setting{NodeAddress: ":50053", HeartbeatTimeout: 1,ElectionTimeout: 10,PeerNodeAddress:[]string{":50051",":50052"}},"mydb3",":50043")


	// 逻辑时钟往后走10，使得选举出leader
	for i:= 0;i < 10;i++{
		kvserver1.r.LogicalTimerChange()
		kvserver2.r.LogicalTimerChange()
		kvserver3.r.LogicalTimerChange()
	}

	assert.Equal(t, kvserver1.r.State, raft.Leader)


	// 测试put方法
	r, err := kv_client.Put(":50041", &pb.RawPutRequest{Key: []byte("hello"), Value: []byte("world")})
	if err != nil {
		log.Println(err)
	}

	log.Printf("error is: %s", r.GetError())


	// 检查节点的数据库当中是否存在数据
	get, err := kvserver1.db.Get([]byte("hello"),nil)
	if err != nil {
		log.Println(err)
	}
	assert.Equal(t, string(get),"world")

	get2, err := kvserver2.db.Get([]byte("hello"),nil)
	if err != nil {
		log.Println(err)
	}
	assert.Equal(t, string(get2),"world")


	// 检查调用Get方法
	r2, err := kv_client.Get(":50042",&pb.RawGetRequest{Key: []byte("hello")})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("value is %v, error is: %s", string(r2.GetValue()),r2.GetError())
	assert.Equal(t, string(r2.GetValue()),"world")


	// 测试再次put一个新的键值对和再一次get
	r3, err := kv_client.Put(":50041", &pb.RawPutRequest{Key: []byte("my_db"), Value: []byte("ok")})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("error is: %s", r3.GetError())


	r4, err := kv_client.Get(":50042",&pb.RawGetRequest{Key: []byte("my_db")})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("value is %v, error is: %s", string(r4.GetValue()),r4.GetError())
	assert.Equal(t, string(r4.GetValue()),"ok")


	// 测试删除的用例
	r5, err := kv_client.Del(":50041",&pb.RawDelRequest{Key: []byte("my_db")})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("error is: %s", r5.GetError())

	r6, err := kv_client.Get(":50042",&pb.RawGetRequest{Key: []byte("my_db")})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("value is %v, error is: %s", string(r6.GetValue()),r6.GetError())
	// 删除后，get返回找不到的错误信息
	assert.Equal(t, r6.NotFound,true)



	// 结束服务器线程
	kvserver1.r.StopRaft()
	kvserver2.r.StopRaft()
	kvserver3.r.StopRaft()

	s1.GracefulStop()
	s2.GracefulStop()
	s3.GracefulStop()
}