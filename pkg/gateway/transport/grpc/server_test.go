package gatewaygrpc

import (
	"context"
	"testing"
	"time"

	"github.com/gopherex/synclog/internal/storage/memory"
	"github.com/gopherex/synclog/pkg/gateway"
	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestToStatusError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "invalid argument", err: synclog.ErrInvalidArgument, code: codes.InvalidArgument},
		{name: "not found", err: synclog.ErrNotFound, code: codes.NotFound},
		{name: "access denied", err: gateway.ErrAccessDenied, code: codes.PermissionDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := toStatusError(tt.err)
			if got := status.Code(err); got != tt.code {
				t.Fatalf("code = %v, want %v", got, tt.code)
			}
		})
	}
}

func TestGatewaySubscribeStreamsBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := memory.NewStore()
	core, err := synclog.NewEngine(store, store)
	if err != nil {
		t.Fatalf("new core: %v", err)
	}
	target := &synclogv1.SyncTarget{Namespace: "project", Id: "777", View: "default"}
	streamID := synclog.StreamID("stream:project:777")
	engine, err := gateway.NewEngine(core, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(func(_ context.Context, actor any, requested synclog.SubscriberID) (synclog.SubscriberID, error) {
			if actor != "actor" || requested != "sub:1" {
				return "", gateway.ErrAccessDenied
			}
			return requested, nil
		}),
		Resolver: resolverFunc(func(_ context.Context, _ any, got *synclogv1.SyncTarget) (gateway.TargetResolution, error) {
			return gateway.TargetResolution{
				Target: got,
				Bindings: []gateway.StreamBinding{{
					StreamID:     streamID,
					PayloadTypes: []string{"project.event"},
				}},
			}, nil
		}),
		Authorizer: authorizerFunc(func(context.Context, gateway.AuthRequest) error {
			return nil
		}),
		PayloadPolicy: payloadPolicyFunc(func(context.Context, any, *synclogv1.SyncTarget, string) error {
			return nil
		}),
		SnapshotPolicy: snapshotPolicyFunc(func(context.Context, any, *synclogv1.SyncTarget, string) error {
			return nil
		}),
		CodecRegistry: gateway.NewStaticCodecRegistry(gateway.CodecRegistryEntry{
			PayloadType:    "project.event",
			PayloadVersion: 1,
		}),
	})
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	if _, err := core.Append(ctx, synclog.AppendRequest{
		StreamID:       streamID,
		Payload:        []byte("event"),
		PayloadType:    "project.event",
		PayloadVersion: 1,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	server, err := NewServer(
		engine,
		WithActorExtractor(func(context.Context) (any, error) { return "actor", nil }),
		WithPollInterval(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	stream := &fakeGatewaySubscribeStream{
		ctx:    ctx,
		cancel: cancel,
	}
	err = server.GatewaySubscribe(&synclogv1.GatewaySubscribeRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	}, stream)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("subscribe err code = %v, want Canceled", status.Code(err))
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent responses = %d, want 1", len(stream.sent))
	}
	resp := stream.sent[0]
	if len(resp.GetBatch().GetEvents()) != 1 || string(resp.GetBatch().GetEvents()[0].GetPayload()) != "event" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

type fakeGatewaySubscribeStream struct {
	grpc.ServerStream
	ctx    context.Context
	cancel context.CancelFunc
	sent   []*synclogv1.GatewaySubscribeResponse
}

func (s *fakeGatewaySubscribeStream) Send(resp *synclogv1.GatewaySubscribeResponse) error {
	s.sent = append(s.sent, resp)
	s.cancel()
	return nil
}

func (s *fakeGatewaySubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeGatewaySubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeGatewaySubscribeStream) SetTrailer(metadata.MD)       {}
func (s *fakeGatewaySubscribeStream) Context() context.Context     { return s.ctx }
func (s *fakeGatewaySubscribeStream) SendMsg(any) error            { return nil }
func (s *fakeGatewaySubscribeStream) RecvMsg(any) error            { return nil }

type subscriberResolverFunc func(context.Context, any, synclog.SubscriberID) (synclog.SubscriberID, error)

func (f subscriberResolverFunc) ResolveSubscriber(ctx context.Context, actor any, requested synclog.SubscriberID) (synclog.SubscriberID, error) {
	return f(ctx, actor, requested)
}

type resolverFunc func(context.Context, any, *synclogv1.SyncTarget) (gateway.TargetResolution, error)

func (f resolverFunc) ResolveTarget(ctx context.Context, actor any, target *synclogv1.SyncTarget) (gateway.TargetResolution, error) {
	return f(ctx, actor, target)
}

type authorizerFunc func(context.Context, gateway.AuthRequest) error

func (f authorizerFunc) Authorize(ctx context.Context, req gateway.AuthRequest) error {
	return f(ctx, req)
}

type payloadPolicyFunc func(context.Context, any, *synclogv1.SyncTarget, string) error

func (f payloadPolicyFunc) AllowPayload(ctx context.Context, actor any, target *synclogv1.SyncTarget, payloadType string) error {
	return f(ctx, actor, target, payloadType)
}

type snapshotPolicyFunc func(context.Context, any, *synclogv1.SyncTarget, string) error

func (f snapshotPolicyFunc) AllowSnapshot(ctx context.Context, actor any, target *synclogv1.SyncTarget, payloadType string) error {
	return f(ctx, actor, target, payloadType)
}
