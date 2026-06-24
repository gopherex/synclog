package gatewaygrpc

import (
	"context"
	"errors"
	"strings"
	"sync"
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

	// subs registers live subscribe streams by subscription_id so the unary
	// ModifySubscription RPC can inject add/remove into an established stream.
	mu   sync.Mutex
	subs map[string]*liveSub
}

// liveSub is the mutable target set backing one live GatewaySubscribe stream.
// ModifySubscription mutates it and wakes the stream loop; the loop reads the
// current set each iteration so adds/removes take effect without a teardown.
type liveSub struct {
	// subscriberID is the requested subscriber_id of the owning stream, used to
	// reject a ModifySubscription that names a different subscriber.
	subscriberID string

	mu      sync.Mutex
	targets map[string]*synclogv1.SyncTarget // keyed by targetKey
	wake    chan struct{}                    // buffered(1): coalesced wakeups
}

func newLiveSub(subscriberID string, initial []*synclogv1.SyncTarget) *liveSub {
	l := &liveSub{
		subscriberID: subscriberID,
		targets:      make(map[string]*synclogv1.SyncTarget, len(initial)),
		wake:         make(chan struct{}, 1),
	}
	for _, t := range initial {
		l.targets[targetKey(t)] = t
	}
	return l
}

func (l *liveSub) snapshot() []*synclogv1.SyncTarget {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*synclogv1.SyncTarget, 0, len(l.targets))
	for _, t := range l.targets {
		out = append(out, t)
	}
	return out
}

// apply adds and removes targets, then wakes the stream loop. Adds win over
// removes when the same target appears in both, matching last-writer semantics.
func (l *liveSub) apply(add []*synclogv1.SyncTarget, remove []*synclogv1.SyncTarget) {
	l.mu.Lock()
	for _, t := range remove {
		delete(l.targets, targetKey(t))
	}
	for _, t := range add {
		l.targets[targetKey(t)] = t
	}
	l.mu.Unlock()
	l.signal()
}

func (l *liveSub) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (s *Server) registerSub(id string, sub *liveSub) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subs[id]; ok {
		return false
	}
	s.subs[id] = sub
	return true
}

func (s *Server) unregisterSub(id string, sub *liveSub) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Guard against unregistering a re-registered id (a new stream may have
	// reclaimed the id after this one ended).
	if s.subs[id] == sub {
		delete(s.subs, id)
	}
}

func (s *Server) lookupSub(id string) *liveSub {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subs[id]
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
		subs:           make(map[string]*liveSub),
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

	// Back the stream with a mutable target set so ModifySubscription can add or
	// remove targets live. With no subscription_id the set is fixed (legacy
	// behavior); with one, the stream is registered for incremental updates.
	sub := newLiveSub(req.GetSubscriberId(), req.GetTargets())
	if id := req.GetSubscriptionId(); id != "" {
		if !s.registerSub(id, sub) {
			return status.Error(codes.AlreadyExists, "subscription_id is already active")
		}
		defer s.unregisterSub(id, sub)
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

		targets := sub.snapshot()
		// Drop delivery state for targets removed since the last iteration so a
		// later re-add resumes cleanly (from its server cursor) rather than being
		// suppressed by a stale delivered/too-long entry.
		pruneDeliveryState(delivered, tooLongSent, targets)

		// No targets (all removed, or opened empty to add later): nothing to tail.
		// Keep the stream alive with heartbeats and block until woken or canceled.
		if len(targets) == 0 {
			now := time.Now()
			if now.Sub(lastHeartbeat) >= s.heartbeatEvery {
				if err := stream.Send(&synclogv1.GatewaySubscribeResponse{
					Heartbeat:        true,
					ServerTimeUnixMs: unixMS(now),
				}); err != nil {
					return toStatusError(err)
				}
				lastHeartbeat = now
			}
			if err := waitWake(ctx, sub.wake, nextWait(s.pollInterval, s.heartbeatEvery, lastHeartbeat)); err != nil {
				return toStatusError(err)
			}
			continue
		}

		pollReq := withTargets(req, targets)
		responses, err := s.engine.GatewaySubscribePoll(ctx, actor, pollReq)
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
			// the next poll instead of busy-looping until the subscriber acks. A
			// ModifySubscription wakeup also breaks the sleep so a freshly added
			// target is polled promptly.
			if err := waitWake(ctx, sub.wake, s.pollInterval); err != nil {
				return toStatusError(err)
			}
			continue
		}
		if err := s.waitOrWake(ctx, actor, pollReq, sub.wake, delivered, nextWait(s.pollInterval, s.heartbeatEvery, lastHeartbeat)); err != nil {
			return toStatusError(err)
		}
	}
}

// waitOrWake blocks in the engine watcher until an event arrives, the timeout
// elapses, the context is canceled, or a ModifySubscription wakeup fires. A
// wakeup cancels the watcher wait and returns nil so the loop re-snapshots the
// (now-changed) target set and polls it — including any freshly added target.
func (s *Server) waitOrWake(ctx context.Context, actor any, req *synclogv1.GatewaySubscribeRequest, wake <-chan struct{}, delivered map[string]uint64, timeout time.Duration) error {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-wake:
			cancel()
		case <-waitCtx.Done():
		}
	}()
	err := s.engine.WaitGatewaySubscribe(waitCtx, actor, req, delivered, timeout)
	if err != nil && ctx.Err() == nil {
		// Parent still alive: the cancellation came from our wakeup, not a real
		// stream teardown — swallow it and let the loop re-poll.
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return err
}

// waitWake blocks until the context is canceled, the timeout elapses, or a
// wakeup fires. It returns ctx.Err() only on cancellation; timeout and wakeup
// both return nil so the caller re-evaluates its state.
func waitWake(ctx context.Context, wake <-chan struct{}, timeout time.Duration) error {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			return nil
		default:
			return nil
		}
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-t.C:
		return nil
	}
}

// withTargets returns a shallow copy of req carrying the given target set, used
// to poll the current (mutable) target set without mutating the original.
func withTargets(req *synclogv1.GatewaySubscribeRequest, targets []*synclogv1.SyncTarget) *synclogv1.GatewaySubscribeRequest {
	return &synclogv1.GatewaySubscribeRequest{
		SubscriberId:         req.GetSubscriberId(),
		Targets:              targets,
		BatchLimitPerTarget:  req.GetBatchLimitPerTarget(),
		MaxInFlightPerTarget: req.GetMaxInFlightPerTarget(),
		SubscriptionId:       req.GetSubscriptionId(),
	}
}

// pruneDeliveryState drops delivered/too-long entries whose target is no longer
// in the active set. Keys are "<targetKey>#<bindingKey>"; an entry is kept only
// if some active target is its targetKey prefix.
func pruneDeliveryState(delivered map[string]uint64, tooLongSent map[string]bool, targets []*synclogv1.SyncTarget) {
	if len(delivered) == 0 && len(tooLongSent) == 0 {
		return
	}
	prefixes := make([]string, 0, len(targets))
	for _, t := range targets {
		prefixes = append(prefixes, targetKey(t)+"#")
	}
	keep := func(key string) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(key, p) {
				return true
			}
		}
		return false
	}
	for key := range delivered {
		if !keep(key) {
			delete(delivered, key)
		}
	}
	for key := range tooLongSent {
		if !keep(key) {
			delete(tooLongSent, key)
		}
	}
}

func (s *Server) ModifySubscription(ctx context.Context, req *synclogv1.ModifySubscriptionRequest) (*synclogv1.ModifySubscriptionResponse, error) {
	actor, err := s.actor(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}
	if req.GetSubscriptionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription_id is required")
	}

	// Resolve and authorize adds eagerly so a rejected target is reported
	// synchronously and never injected into the stream. A subscriber-resolution
	// failure is fatal (whole call fails); per-target failures are isolated.
	added, rejected, err := s.engine.ResolveSubscribeTargets(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()), req.GetAddTargets())
	if err != nil {
		return nil, toStatusError(err)
	}

	// Locate the live stream and verify ownership. Fail closed (NotFound) when
	// there is no active stream for the id or it belongs to another subscriber,
	// so a caller cannot probe or mutate another subscriber's subscription.
	sub := s.lookupSub(req.GetSubscriptionId())
	if sub == nil || sub.subscriberID != req.GetSubscriberId() {
		return nil, status.Error(codes.NotFound, "no active subscription for subscription_id")
	}

	accepted := make([]*synclogv1.SyncTarget, 0, len(added))
	for _, state := range added {
		accepted = append(accepted, state.GetTarget())
	}
	sub.apply(accepted, req.GetRemoveTargets())

	return &synclogv1.ModifySubscriptionResponse{
		Added:    added,
		Rejected: rejected,
	}, nil
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
