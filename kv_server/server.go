package kv_server

import (
	"Distributed_key_value_system/raft"
	"context"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"net"

	pb "Distributed_key_value_system/kvrpc"
	"google.golang.org/grpc"
)

type KVserver struct {
	pb.KVrpcServer
	r  *raft.Raft
	db *leveldb.DB
}

func (s *KVserver) RawPut(ctx context.Context, in *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	log.Printf("Received: key is %v, value is %v", string(in.GetKey()), string(in.GetValue()))
	kventry := raft.KVentry{}
	kventry.KVOperation = raft.Put
	kventry.Key = in.GetKey()
	kventry.Value = in.GetValue()
	kventryByte, _ := json.Marshal(kventry)
	flag := s.r.Append(kventryByte)
	if flag {
		err := s.db.Put(in.GetKey(), in.GetValue(), nil)
		if err != nil {
			log.Println("RawPut error is ", err)
			return &pb.RawPutResponse{Error: fmt.Sprintf("err is %v", err)}, nil
		}
		return &pb.RawPutResponse{Error: ""}, nil
	} else {
		return &pb.RawPutResponse{Error: "this server node can't exec put"}, nil
	}

}

func (s *KVserver) RawGet(ctx context.Context, in *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	log.Printf("Received: key is %v", string(in.GetKey()))

	value, err := s.db.Get(in.GetKey(), nil)
	if err != nil {
		log.Println("RawGet error is ", err)
		if err == leveldb.ErrNotFound {
			return &pb.RawGetResponse{Value: value, Error: "", NotFound: true}, nil
		}
		return &pb.RawGetResponse{Value: value, Error: fmt.Sprintf("err is %v", err), NotFound: false}, nil
	}
	return &pb.RawGetResponse{Value: value, Error: "", NotFound: false}, nil
}

func (s *KVserver) RawDel(ctx context.Context, in *pb.RawDelRequest) (*pb.RawDelResponse, error) {
	log.Printf("Received: key is %v", string(in.GetKey()))

	kventry := raft.KVentry{}
	kventry.KVOperation = raft.Del
	kventry.Key = in.GetKey()
	kventryByte, _ := json.Marshal(kventry)
	flag := s.r.Append(kventryByte)
	if flag {
		err := s.db.Delete(in.GetKey(), nil)
		if err != nil {
			log.Println("RawPut error is ", err)
			return &pb.RawDelResponse{Error: fmt.Sprintf("err is %v", err)}, nil
		}
		return &pb.RawDelResponse{Error: ""}, nil
	} else {
		return &pb.RawDelResponse{Error: "this server node can't exec put"}, nil
	}

	//err := s.db.Delete(in.GetKey(), nil)
	//if err != nil {
	//	log.Println("RawDel error is ",err)
	//	return &pb.RawDelResponse{Error: fmt.Sprintf("err is %v",err)}, nil
	//}
	//return &pb.RawDelResponse{Error: ""}, nil
}

type KVserverSetting struct {
	raftsetting *raft.Setting
	dbname      string
}

func (s *KVserver) SetupServer(setting *KVserverSetting) {

	r := raft.Raft{}
	r.NewRaft(setting.raftsetting)
	s.r = &r
	var err error
	s.db, err = leveldb.OpenFile("./"+setting.dbname, nil)
	if err != nil {
		log.Fatalln(err)
	}
	s.r.ConnectDB(s.db)
	//defer func(db *leveldb.DB) {
	//	err := db.Close()
	//	if err != nil {
	//
	//	}
	//}(s.db)
}

// NewServer 启动新的kvserver
func NewServer(raftsetting *raft.Setting, dbname string, nodeAddress string) (*grpc.Server, *KVserver) {
	lis, err := net.Listen("tcp", nodeAddress)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	log.Printf("server listening at %v", lis.Addr())
	kv_server := KVserver{}
	kv_setting := KVserverSetting{raftsetting, dbname}
	kv_server.SetupServer(&kv_setting)
	pb.RegisterKVrpcServer(s, &kv_server)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return s, &kv_server
}

func (s *KVserver) StopServer() {
	s.r.StopRaft()
}
