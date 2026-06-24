package gatewaygrpc

import (
	"context"
	"slices"
	"sync"
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

// collectStream is a thread-safe fake server stream that records every response
// and keeps the stream open (unlike fakeGatewaySubscribeStream which cancels on
// first Send) so incremental add/remove can be exercised on a live stream.
type collectStream struct {
	grpc.ServerStream
	ctx context.Context

	mu   sync.Mutex
	sent []*synclogv1.GatewaySubscribeResponse
}

func (s *collectStream) Send(resp *synclogv1.GatewaySubscribeResponse) error {
	s.mu.Lock()
	s.sent = append(s.sent, resp)
	s.mu.Unlock()
	return nil
}

func (s *collectStream) Context() context.Context     { return s.ctx }
func (s *collectStream) SetHeader(metadata.MD) error  { return nil }
func (s *collectStream) SendHeader(metadata.MD) error { return nil }
func (s *collectStream) SetTrailer(metadata.MD)       {}
func (s *collectStream) SendMsg(any) error            { return nil }
func (s *collectStream) RecvMsg(any) error            { return nil }

// batchPayloads returns the payloads delivered for the given target id, in
// delivery order, so a test can assert what a target's stream has received.
func (s *collectStream) batchPayloads(targetID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, resp := range s.sent {
		batch := resp.GetBatch()
		if batch == nil || batch.GetTarget().GetId() != targetID {
			continue
		}
		for _, ev := range batch.GetEvents() {
			out = append(out, string(ev.GetPayload()))
		}
	}
	return out
}

func (s *collectStream) sawTooLong(targetID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, resp := range s.sent {
		if resp.GetStatus() != synclogv1.CatchUpStatus_CATCH_UP_STATUS_TOO_LONG {
			continue
		}
		if resp.GetState().GetTarget().GetId() == targetID {
			return true
		}
	}
	return false
}

func contains(haystack []string, needle string) bool {
	return slices.Contains(haystack, needle)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func streamIDFor(target *synclogv1.SyncTarget) synclog.StreamID {
	return synclog.StreamID("stream:" + target.GetNamespace() + ":" + target.GetId())
}

// modifyTestEnv wires a gateway engine + grpc server over a memory core where
// every target resolves to its own stream ("stream:<ns>:<id>"). The authorizer
// denies any target whose id is in denyIDs, to exercise per-target isolation.
type modifyTestEnv struct {
	core   *synclog.Engine
	store  *memory.Store
	server *Server
}

func newModifyTestEnv(t *testing.T, denyIDs ...string) *modifyTestEnv {
	t.Helper()
	store := memory.NewStore()
	core, err := synclog.NewEngine(store, store)
	if err != nil {
		t.Fatalf("new core: %v", err)
	}
	denied := make(map[string]bool, len(denyIDs))
	for _, id := range denyIDs {
		denied[id] = true
	}
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
					StreamID:     streamIDFor(got),
					PayloadTypes: []string{"project.event"},
				}},
			}, nil
		}),
		Authorizer: authorizerFunc(func(_ context.Context, req gateway.AuthRequest) error {
			if denied[req.Target.GetId()] {
				return gateway.ErrAccessDenied
			}
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
	server, err := NewServer(
		engine,
		WithActorExtractor(func(context.Context) (any, error) { return "actor", nil }),
		WithPollInterval(time.Millisecond),
		WithHeartbeatInterval(time.Hour), // keep heartbeats out of these tests
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return &modifyTestEnv{core: core, store: store, server: server}
}

func (env *modifyTestEnv) append(t *testing.T, target *synclogv1.SyncTarget, payload string) {
	t.Helper()
	if _, err := env.core.Append(context.Background(), synclog.AppendRequest{
		StreamID:       streamIDFor(target),
		Payload:        []byte(payload),
		PayloadType:    "project.event",
		PayloadVersion: 1,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
}

// runStream starts GatewaySubscribe in a goroutine and returns the stream plus a
// stop func that cancels it and waits for the handler to return.
func (env *modifyTestEnv) runStream(t *testing.T, req *synclogv1.GatewaySubscribeRequest) (*collectStream, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stream := &collectStream{ctx: ctx}
	done := make(chan struct{})
	go func() {
		_ = env.server.GatewaySubscribe(req, stream)
		close(done)
	}()
	return stream, func() {
		cancel()
		<-done
	}
}

func mkTarget(id string) *synclogv1.SyncTarget {
	return &synclogv1.SyncTarget{Namespace: "project", Id: id, View: "default"}
}

func TestModifySubscriptionAddsTargetToLiveStream(t *testing.T) {
	env := newModifyTestEnv(t)
	a := mkTarget("A")
	b := mkTarget("B")
	env.append(t, a, "a1")

	stream, stop := env.runStream(t, &synclogv1.GatewaySubscribeRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		Targets:        []*synclogv1.SyncTarget{a},
	})
	defer stop()

	waitFor(t, "A backlog delivered", func() bool {
		return len(stream.batchPayloads("A")) == 1
	})

	// Add B live: its backlog must be injected without touching A.
	env.append(t, b, "b1")
	resp, err := env.server.ModifySubscription(context.Background(), &synclogv1.ModifySubscriptionRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		AddTargets:     []*synclogv1.SyncTarget{b},
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	if len(resp.GetAdded()) != 1 || len(resp.GetRejected()) != 0 {
		t.Fatalf("added=%d rejected=%d, want 1/0", len(resp.GetAdded()), len(resp.GetRejected()))
	}

	waitFor(t, "B backlog delivered", func() bool {
		return len(stream.batchPayloads("B")) == 1
	})

	// A keeps flowing after B was added. (Without an ack A's cursor stays at 0,
	// so catch-up may re-send a1 in the batch; the client dedups per event. We
	// only assert a2 eventually arrives on A's stream.)
	env.append(t, a, "a2")
	waitFor(t, "A continues uninterrupted", func() bool {
		return contains(stream.batchPayloads("A"), "a2")
	})
}

func TestModifySubscriptionRemovesTargetAndResumesOnReAdd(t *testing.T) {
	env := newModifyTestEnv(t)
	a := mkTarget("A")
	env.append(t, a, "a1")

	stream, stop := env.runStream(t, &synclogv1.GatewaySubscribeRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		Targets:        []*synclogv1.SyncTarget{a},
	})
	defer stop()

	waitFor(t, "A delivered", func() bool { return len(stream.batchPayloads("A")) == 1 })

	// Ack a1 so a later re-add resumes from cursor (no duplicate of a1).
	if _, err := env.server.GatewayAck(context.Background(), &synclogv1.GatewayAckRequest{
		SubscriberId: "sub:1",
		Target:       a,
		Seq:          1,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Remove A: it must stop flowing.
	if _, err := env.server.ModifySubscription(context.Background(), &synclogv1.ModifySubscriptionRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		RemoveTargets:  []*synclogv1.SyncTarget{a},
	}); err != nil {
		t.Fatalf("modify remove: %v", err)
	}

	// Let the removal settle past the snapshot→poll lag (one iteration), then
	// append: a target removed before the event is appended must not deliver it.
	time.Sleep(50 * time.Millisecond)
	env.append(t, a, "a2")
	time.Sleep(50 * time.Millisecond)
	if got := stream.batchPayloads("A"); contains(got, "a2") {
		t.Fatalf("A delivered a2 (%v) after removal", got)
	}

	// Re-add A: resumes from the acked cursor (seq 1), so only a2 arrives.
	if _, err := env.server.ModifySubscription(context.Background(), &synclogv1.ModifySubscriptionRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		AddTargets:     []*synclogv1.SyncTarget{a},
	}); err != nil {
		t.Fatalf("modify re-add: %v", err)
	}
	waitFor(t, "A resumes from cursor", func() bool {
		return contains(stream.batchPayloads("A"), "a2")
	})
}

func TestModifySubscriptionIsolatesRejectedTarget(t *testing.T) {
	env := newModifyTestEnv(t, "DENY")
	a := mkTarget("A")
	deny := mkTarget("DENY")
	env.append(t, a, "a1")

	stream, stop := env.runStream(t, &synclogv1.GatewaySubscribeRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		Targets:        []*synclogv1.SyncTarget{a},
	})
	defer stop()
	waitFor(t, "A delivered", func() bool { return len(stream.batchPayloads("A")) == 1 })

	resp, err := env.server.ModifySubscription(context.Background(), &synclogv1.ModifySubscriptionRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		AddTargets:     []*synclogv1.SyncTarget{deny},
	})
	if err != nil {
		t.Fatalf("modify: %v", err)
	}
	if len(resp.GetAdded()) != 0 || len(resp.GetRejected()) != 1 {
		t.Fatalf("added=%d rejected=%d, want 0/1", len(resp.GetAdded()), len(resp.GetRejected()))
	}
	if got := resp.GetRejected()[0].GetCode(); got != "PERMISSION_DENIED" {
		t.Fatalf("rejection code = %q, want PERMISSION_DENIED", got)
	}

	// Stream survives and A keeps flowing despite the rejected add.
	env.append(t, a, "a2")
	waitFor(t, "A continues after rejection", func() bool {
		return contains(stream.batchPayloads("A"), "a2")
	})
}

func TestModifySubscriptionUnknownSubscription(t *testing.T) {
	env := newModifyTestEnv(t)
	_, err := env.server.ModifySubscription(context.Background(), &synclogv1.ModifySubscriptionRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "does-not-exist",
		AddTargets:     []*synclogv1.SyncTarget{mkTarget("A")},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}

func TestModifySubscriptionTooLongOnAddedTarget(t *testing.T) {
	env := newModifyTestEnv(t)
	ctx := context.Background()
	a := mkTarget("A")
	b := mkTarget("B")
	env.append(t, a, "a1")

	// Build B with a truncated backlog so its catch-up reports TOO_LONG.
	for range 5 {
		env.append(t, b, "b")
	}
	if _, err := env.store.TruncateBefore(ctx, streamIDFor(b), 4); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	stream, stop := env.runStream(t, &synclogv1.GatewaySubscribeRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		Targets:        []*synclogv1.SyncTarget{a},
	})
	defer stop()
	waitFor(t, "A delivered", func() bool { return len(stream.batchPayloads("A")) == 1 })

	if _, err := env.server.ModifySubscription(ctx, &synclogv1.ModifySubscriptionRequest{
		SubscriberId:   "sub:1",
		SubscriptionId: "live:1",
		AddTargets:     []*synclogv1.SyncTarget{b},
	}); err != nil {
		t.Fatalf("modify: %v", err)
	}

	waitFor(t, "B reports TOO_LONG", func() bool {
		return stream.sawTooLong("B")
	})
}

func TestPruneDeliveryState(t *testing.T) {
	delivered := map[string]uint64{
		"project/A/default#default": 3,
		"project/B/default#default": 7,
	}
	tooLong := map[string]bool{
		"project/A/default#default": true,
		"project/B/default#default": true,
	}
	// Keep only A.
	pruneDeliveryState(delivered, tooLong, []*synclogv1.SyncTarget{mkTarget("A")})
	if _, ok := delivered["project/B/default#default"]; ok {
		t.Fatalf("B delivered not pruned: %v", delivered)
	}
	if _, ok := delivered["project/A/default#default"]; !ok {
		t.Fatalf("A delivered wrongly pruned: %v", delivered)
	}
	if _, ok := tooLong["project/B/default#default"]; ok {
		t.Fatalf("B tooLong not pruned: %v", tooLong)
	}
}
