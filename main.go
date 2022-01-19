package main

import (
	"Distributed_key_value_system/kv_server"
	"Distributed_key_value_system/raft"
)

func main() {
	// start server
	s1, kvserver1 := kv_server.NewServer(&raft.Setting{NodeAddress: ":50051",
		HeartbeatTimeout: 1,
		ElectionTimeout:  3,
		PeerNodeAddress:  []string{":50052", ":50053"}},
		"mydb1",
		":50041")

	defer func() {
		s1.GracefulStop()
		kvserver1.StopServer()
	}()

	//client function demo

	/*r, err := kv_client.Put(":50041", &pb.RawPutRequest{Key: []byte("hello"), Value: []byte("world")})
	if err != nil {
		log.Println(err)
	}
	fmt.Println(r)*/

}
