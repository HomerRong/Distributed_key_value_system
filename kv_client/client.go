
package kv_client

import (
	_ "bytes"
	"context"
	"log"
	"time"

	pb "Distributed_key_value_system/kvrpc"
	"google.golang.org/grpc"
)



func Put(address string,in *pb.RawPutRequest)(*pb.RawPutResponse, error){
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)
	c := pb.NewKVrpcClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.RawPut(ctx,in)

	return r, err
}


func Get(address string,in *pb.RawGetRequest)(*pb.RawGetResponse, error){
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)
	c := pb.NewKVrpcClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.RawGet(ctx,in)

	return r, err
}


func Del(address string,in *pb.RawDelRequest)(*pb.RawDelResponse, error){
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			return
		}
	}(conn)
	c := pb.NewKVrpcClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.RawDel(ctx,in)

	return r, err
}