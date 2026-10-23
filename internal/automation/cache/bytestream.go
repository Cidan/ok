package cache

import (
	"context"

	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

// byteStream is ByteStream's server, as the cache serves it. The generated package registers it
// only on a *grpc.Server; byteStreamDesc is the same service, for any registrar.
type byteStream interface {
	Read(*bspb.ReadRequest, grpc.ServerStreamingServer[bspb.ReadResponse]) error
	Write(grpc.ClientStreamingServer[bspb.WriteRequest, bspb.WriteResponse]) error
	QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error)
}

var byteStreamDesc = grpc.ServiceDesc{
	ServiceName: "google.bytestream.ByteStream",
	HandlerType: (*byteStream)(nil),
	Methods: []grpc.MethodDesc{{MethodName: "QueryWriteStatus", Handler: func(srv any, ctx context.Context, dec func(any) error,
		interceptor grpc.UnaryServerInterceptor) (any, error) {
		in := new(bspb.QueryWriteStatusRequest)
		if err := dec(in); err != nil {
			return nil, err
		}
		call := func(ctx context.Context, req any) (any, error) {
			return srv.(byteStream).QueryWriteStatus(ctx, req.(*bspb.QueryWriteStatusRequest))
		}
		if interceptor == nil {
			return call(ctx, in)
		}
		return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/google.bytestream.ByteStream/QueryWriteStatus"}, call)
	}}},
	Streams: []grpc.StreamDesc{
		{StreamName: "Read", ServerStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			in := new(bspb.ReadRequest)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			return srv.(byteStream).Read(in, &grpc.GenericServerStream[bspb.ReadRequest, bspb.ReadResponse]{ServerStream: stream})
		}},
		{StreamName: "Write", ClientStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(byteStream).Write(&grpc.GenericServerStream[bspb.WriteRequest, bspb.WriteResponse]{ServerStream: stream})
		}},
	},
	Metadata: "google/bytestream/bytestream.proto",
}
