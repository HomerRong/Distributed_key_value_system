// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package raftrpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// RaftrpcClient is the client API for Raftrpc service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RaftrpcClient interface {
	HandleMessage(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Response, error)
}

type raftrpcClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftrpcClient(cc grpc.ClientConnInterface) RaftrpcClient {
	return &raftrpcClient{cc}
}

func (c *raftrpcClient) HandleMessage(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/raftrpc.Raftrpc/HandleMessage", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftrpcServer is the server API for Raftrpc service.
// All implementations must embed UnimplementedRaftrpcServer
// for forward compatibility
type RaftrpcServer interface {
	HandleMessage(context.Context, *Message) (*Response, error)
	mustEmbedUnimplementedRaftrpcServer()
}

// UnimplementedRaftrpcServer must be embedded to have forward compatible implementations.
type UnimplementedRaftrpcServer struct {
}

func (UnimplementedRaftrpcServer) HandleMessage(context.Context, *Message) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method HandleMessage not implemented")
}
func (UnimplementedRaftrpcServer) mustEmbedUnimplementedRaftrpcServer() {}

// UnsafeRaftrpcServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftrpcServer will
// result in compilation errors.
type UnsafeRaftrpcServer interface {
	mustEmbedUnimplementedRaftrpcServer()
}

func RegisterRaftrpcServer(s grpc.ServiceRegistrar, srv RaftrpcServer) {
	s.RegisterService(&Raftrpc_ServiceDesc, srv)
}

func _Raftrpc_HandleMessage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftrpcServer).HandleMessage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raftrpc.Raftrpc/HandleMessage",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftrpcServer).HandleMessage(ctx, req.(*Message))
	}
	return interceptor(ctx, in, info, handler)
}

// Raftrpc_ServiceDesc is the grpc.ServiceDesc for Raftrpc service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Raftrpc_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raftrpc.Raftrpc",
	HandlerType: (*RaftrpcServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "HandleMessage",
			Handler:    _Raftrpc_HandleMessage_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raftrpc/raftrpc.proto",
}
