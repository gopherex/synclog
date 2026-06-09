package gatewaygrpc

import (
	"context"
	"errors"
	"time"

	"github.com/gopherex/synclog/pkg/gateway"
	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ActorExtractor func(context.Context) (any, error)

type Server struct {
	synclogv1.UnimplementedSyncGatewayServiceServer

	engine         *gateway.Engine
	actorExtractor ActorExtractor
	pollInterval   time.Duration
	heartbeatEvery time.Duration
}

type ServerOption func(*Server)

func WithActorExtractor(extractor ActorExtractor) ServerOption {
	return func(s *Server) {
		if extractor != nil {
			s.actorExtractor = extractor
		}
	}
}

func WithPollInterval(interval time.Duration) ServerOption {
	return func(s *Server) {
		if interval > 0 {
			s.pollInterval = interval
		}
	}
}

func WithHeartbeatInterval(interval time.Duration) ServerOption {
	return func(s *Server) {
		if interval > 0 {
			s.heartbeatEvery = interval
		}
	}
}

func NewServer(engine *gateway.Engine, opts ...ServerOption) (*Server, error) {
	if engine == nil {
		return nil, status.Error(codes.InvalidArgument, "gateway engine is required")
	}
	s := &Server{
		engine: engine,
		// Fail closed: the gateway is product-facing, so a missing actor extractor
		// must reject requests rather than silently serve them as an anonymous
		// actor. Products opt in to a real identity via WithActorExtractor.
		actorExtractor: func(context.Context) (any, error) {
			return nil, status.Error(codes.Unauthenticated, "no actor extractor configured")
		},
		pollInterval:   100 * time.Millisecond,
		heartbeatEvery: 15 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func Register(registrar grpc.ServiceRegistrar, engine *gateway.Engine, opts ...ServerOption) error {
	server, err := NewServer(engine, opts...)
	if err != nil {
		return err
	}
	synclogv1.RegisterSyncGatewayServiceServer(registrar, server)
	return nil
}

func (s *Server) Open(ctx context.Context, req *synclogv1.OpenRequest) (*synclogv1.OpenResponse, error) {
	actor, err := s.actor(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp, err := s.engine.Open(ctx, actor, req)
	if err != nil {
		return nil, toStatusError(err)
	}
	return resp, nil
}

func (s *Server) GatewayCatchUp(ctx context.Context, req *synclogv1.GatewayCatchUpRequest) (*synclogv1.GatewayCatchUpResponse, error) {
	actor, err := s.actor(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp, err := s.engine.GatewayCatchUp(ctx, actor, req)
	if err != nil {
		return nil, toStatusError(err)
	}
	return resp, nil
}

func (s *Server) GatewaySubscribe(req *synclogv1.GatewaySubscribeRequest, stream grpc.ServerStreamingServer[synclogv1.GatewaySubscribeResponse]) error {
	ctx := stream.Context()
	actor, err := s.actor(ctx)
	if err != nil {
		return toStatusError(err)
	}

	delivered := make(map[string]uint64)
	tooLongSent := make(map[string]bool)
	lastHeartbeat := time.Now()

	for {
		select {
		case <-ctx.Done():
			return toStatusError(ctx.Err())
		default:
		}

		responses, err := s.engine.GatewaySubscribePoll(ctx, actor, req)
		if err != nil {
			return toStatusError(err)
		}

		sent := false
		hasMore := false
		throttled := false
		for _, resp := range responses {
			key := responseKey(resp)
			state := resp.GetState()
			if state != nil {
				cursorSeq := bindingCursorSeq(state, resp.GetBindingKey())
				if cursorSeq >= delivered[key] {
					delivered[key] = cursorSeq
					tooLongSent[key] = false
				}
				if req.GetMaxInFlightPerTarget() > 0 && delivered[key] > cursorSeq &&
					delivered[key]-cursorSeq >= uint64(req.GetMaxInFlightPerTarget()) {
					throttled = true
					continue
				}
			}

			if resp.GetStatus() == synclogv1.CatchUpStatus_CATCH_UP_STATUS_TOO_LONG {
				if tooLongSent[key] {
					continue
				}
				tooLongSent[key] = true
				resp.ServerTimeUnixMs = unixMS(time.Now())
				if err := stream.Send(resp); err != nil {
					return toStatusError(err)
				}
				sent = true
				continue
			}

			batch := resp.GetBatch()
			if batch == nil || len(batch.GetEvents()) == 0 {
				continue
			}
			if batch.GetSeq() <= delivered[key] {
				continue
			}
			resp.ServerTimeUnixMs = unixMS(time.Now())
			if err := stream.Send(resp); err != nil {
				return toStatusError(err)
			}
			delivered[key] = batch.GetSeq()
			sent = true
			if !batch.GetFinal() {
				hasMore = true
			}
		}

		now := time.Now()
		if !sent && now.Sub(lastHeartbeat) >= s.heartbeatEvery {
			if err := stream.Send(&synclogv1.GatewaySubscribeResponse{
				Heartbeat:        true,
				ServerTimeUnixMs: unixMS(now),
			}); err != nil {
				return toStatusError(err)
			}
			lastHeartbeat = now
		}

		if hasMore {
			continue
		}
		if throttled {
			// Backpressure: events are available but the subscriber has too many
			// unacked in flight. WaitGatewaySubscribe would return immediately
			// (the head is already ahead of the delivered cursor), so sleep until
			// the next poll instead of busy-looping until the subscriber acks.
			if err := sleepCtx(ctx, s.pollInterval); err != nil {
				return toStatusError(err)
			}
			continue
		}
		if err := s.engine.WaitGatewaySubscribe(ctx, actor, req, delivered, nextWait(s.pollInterval, s.heartbeatEvery, lastHeartbeat)); err != nil {
			return toStatusError(err)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *Server) GatewayAck(ctx context.Context, req *synclogv1.GatewayAckRequest) (*synclogv1.GatewayAckResponse, error) {
	actor, err := s.actor(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp, err := s.engine.GatewayAck(ctx, actor, req)
	if err != nil {
		return nil, toStatusError(err)
	}
	return resp, nil
}

func (s *Server) GatewayGetLatestSnapshot(ctx context.Context, req *synclogv1.GatewayGetLatestSnapshotRequest) (*synclogv1.GatewayGetLatestSnapshotResponse, error) {
	actor, err := s.actor(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp, err := s.engine.GatewayGetLatestSnapshot(ctx, actor, req)
	if err != nil {
		return nil, toStatusError(err)
	}
	return resp, nil
}

func (s *Server) actor(ctx context.Context) (any, error) {
	return s.actorExtractor(ctx)
}

func toStatusError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, synclog.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, synclog.ErrTooLong):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, synclog.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, gateway.ErrAccessDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		// Product-facing surface: do not leak internal/driver error strings.
		return status.Error(codes.Internal, "internal error")
	}
}

func responseKey(resp *synclogv1.GatewaySubscribeResponse) string {
	bindingKey := resp.GetBindingKey()
	if resp.GetState() != nil {
		return targetBindingKey(resp.GetState().GetTarget(), bindingKey)
	}
	if resp.GetBatch() != nil {
		if bindingKey == "" {
			bindingKey = resp.GetBatch().GetBindingKey()
		}
		return targetBindingKey(resp.GetBatch().GetTarget(), bindingKey)
	}
	return ""
}

func bindingCursorSeq(state *synclogv1.TargetState, bindingKey string) uint64 {
	if state == nil {
		return 0
	}
	for _, binding := range state.GetBindings() {
		if binding.GetBindingKey() == bindingKey {
			return binding.GetCursorSeq()
		}
	}
	return state.GetCursorSeq()
}

func targetBindingKey(target *synclogv1.SyncTarget, bindingKey string) string {
	return targetKey(target) + "#" + bindingKey
}

func targetKey(target *synclogv1.SyncTarget) string {
	if target == nil {
		return ""
	}
	return target.GetNamespace() + "/" + target.GetId() + "/" + target.GetView()
}

func unixMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func nextWait(pollInterval time.Duration, heartbeatEvery time.Duration, lastHeartbeat time.Time) time.Duration {
	timeout := pollInterval
	if heartbeatEvery > 0 {
		untilHeartbeat := heartbeatEvery - time.Since(lastHeartbeat)
		if untilHeartbeat <= 0 {
			// Heartbeat overdue: wake promptly so the next idle cycle emits it.
			// Returning 0 would mean "no deadline" to WaitGatewaySubscribe and
			// could block an idle stream indefinitely with no keepalive.
			return pollInterval
		}
		if timeout <= 0 || untilHeartbeat < timeout {
			timeout = untilHeartbeat
		}
	}
	return timeout
}
