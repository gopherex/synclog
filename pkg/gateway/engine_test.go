package gateway_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gopherex/synclog/internal/storage/memory"
	"github.com/gopherex/synclog/pkg/gateway"
	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
)

var errDenied = gateway.ErrAccessDenied

func TestEngineOpenAndCatchUp(t *testing.T) {
	ctx := context.Background()
	target := pbTarget("project", "777", "default")
	streamID := synclog.StreamID("stream:project:777")
	store, core := newCore(t)
	engine := newGateway(t, core, store, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(resolveOnly("user:1", "sub:1")),
		Resolver: &resolver{bindings: map[string][]gateway.StreamBinding{
			targetKey(target): {{
				StreamID:      streamID,
				PayloadTypes:  []string{"project.event"},
				SnapshotTypes: []string{"project.snapshot"},
			}},
		}},
		Authorizer:     authorizerFunc(allowOnly("user:1", "sub:1")),
		PayloadPolicy:  payloadPolicyFunc(allowPayloads("project.event")),
		SnapshotPolicy: snapshotPolicyFunc(allowPayloads("project.snapshot")),
		CodecRegistry:  codecRegistry{"project.event": {1: true}, "project.snapshot": {1: true}},
	})

	if _, err := core.Append(ctx, synclog.AppendRequest{
		StreamID:       streamID,
		Payload:        []byte("event-1"),
		PayloadType:    "project.event",
		PayloadVersion: 1,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	open, err := engine.Open(ctx, "user:1", &synclogv1.OpenRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(open.GetTargets()) != 1 || open.GetTargets()[0].GetHeadSeq() != 1 || open.GetTargets()[0].GetCursorSeq() != 0 {
		t.Fatalf("unexpected open response: %+v", open)
	}

	catchUp, err := engine.GatewayCatchUp(ctx, "user:1", &synclogv1.GatewayCatchUpRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("catch up: %v", err)
	}
	if len(catchUp.GetResults()) != 1 {
		t.Fatalf("result count = %d, want 1", len(catchUp.GetResults()))
	}
	result := catchUp.GetResults()[0]
	if result.GetStatus() != synclogv1.CatchUpStatus_CATCH_UP_STATUS_OK || len(result.GetBatch().GetEvents()) != 1 {
		t.Fatalf("unexpected catch up result: %+v", result)
	}
	event := result.GetBatch().GetEvents()[0]
	if event.GetTarget().GetId() != target.GetId() || event.GetSeq() != 1 || string(event.GetPayload()) != "event-1" {
		t.Fatalf("unexpected gateway event: %+v", event)
	}
}

func TestEngineSupportsMultiBindingTarget(t *testing.T) {
	ctx := context.Background()
	target := pbTarget("project", "777", "default")
	tasksStream := synclog.StreamID("stream:project:777:tasks")
	commentsStream := synclog.StreamID("stream:project:777:comments")
	store, core := newCore(t)
	engine := newGateway(t, core, store, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(resolveOnly("user:1", "sub:1")),
		Resolver: &resolver{bindings: map[string][]gateway.StreamBinding{
			targetKey(target): {
				{BindingKey: "tasks", StreamID: tasksStream, PayloadTypes: []string{"project.task"}},
				{BindingKey: "comments", StreamID: commentsStream, PayloadTypes: []string{"project.comment"}},
			},
		}},
		Authorizer:     authorizerFunc(allowOnly("user:1", "sub:1")),
		PayloadPolicy:  payloadPolicyFunc(allowPayloads("project.task", "project.comment")),
		SnapshotPolicy: snapshotPolicyFunc(allowPayloads("project.snapshot")),
		CodecRegistry:  codecRegistry{"project.task": {1: true}, "project.comment": {1: true}},
	})

	if _, err := core.Append(ctx, synclog.AppendRequest{
		StreamID:       tasksStream,
		PayloadType:    "project.task",
		PayloadVersion: 1,
		Payload:        []byte("task"),
	}); err != nil {
		t.Fatalf("append task: %v", err)
	}
	if _, err := core.Append(ctx, synclog.AppendRequest{
		StreamID:       commentsStream,
		PayloadType:    "project.comment",
		PayloadVersion: 1,
		Payload:        []byte("comment"),
	}); err != nil {
		t.Fatalf("append comment: %v", err)
	}

	open, err := engine.Open(ctx, "user:1", &synclogv1.OpenRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(open.GetTargets()) != 1 || len(open.GetTargets()[0].GetBindings()) != 2 {
		t.Fatalf("unexpected open response: %+v", open)
	}

	catchUp, err := engine.GatewayCatchUp(ctx, "user:1", &synclogv1.GatewayCatchUpRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("catch up: %v", err)
	}
	results := map[string]*synclogv1.GatewayCatchUpResult{}
	for _, result := range catchUp.GetResults() {
		results[result.GetBindingKey()] = result
	}
	for _, bindingKey := range []string{"tasks", "comments"} {
		result := results[bindingKey]
		if result == nil || result.GetBatch().GetBindingKey() != bindingKey || len(result.GetBatch().GetEvents()) != 1 {
			t.Fatalf("unexpected result for %s: %+v", bindingKey, result)
		}
		if result.GetBatch().GetEvents()[0].GetBindingKey() != bindingKey {
			t.Fatalf("event binding key = %q, want %q", result.GetBatch().GetEvents()[0].GetBindingKey(), bindingKey)
		}
	}

	_, err = engine.GatewayAck(ctx, "user:1", &synclogv1.GatewayAckRequest{
		SubscriberId: "sub:1",
		Target:       target,
		Seq:          1,
	})
	if !errors.Is(err, synclog.ErrInvalidArgument) {
		t.Fatalf("ack without binding key err = %v, want invalid argument", err)
	}

	if _, err := engine.GatewayAck(ctx, "user:1", &synclogv1.GatewayAckRequest{
		SubscriberId: "sub:1",
		Target:       target,
		BindingKey:   "tasks",
		Seq:          1,
	}); err != nil {
		t.Fatalf("ack tasks: %v", err)
	}
	tasksCursor, err := core.GetCursor(ctx, "sub:1", tasksStream)
	if err != nil {
		t.Fatalf("get tasks cursor: %v", err)
	}
	commentsCursor, err := core.GetCursor(ctx, "sub:1", commentsStream)
	if err != nil {
		t.Fatalf("get comments cursor: %v", err)
	}
	if tasksCursor.Cursor.Seq != 1 || commentsCursor.Cursor.Seq != 0 {
		t.Fatalf("cursors tasks/comments = %d/%d, want 1/0", tasksCursor.Cursor.Seq, commentsCursor.Cursor.Seq)
	}
}

func TestEngineCatchUpEnforcesPayloadPolicy(t *testing.T) {
	ctx := context.Background()
	target := pbTarget("project", "777", "default")
	streamID := synclog.StreamID("stream:project:777")
	store, core := newCore(t)
	engine := newGateway(t, core, store, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(resolveOnly("user:1", "sub:1")),
		Resolver: &resolver{bindings: map[string][]gateway.StreamBinding{
			targetKey(target): {{StreamID: streamID, PayloadTypes: []string{"project.secret"}}},
		}},
		Authorizer:     authorizerFunc(allowOnly("user:1", "sub:1")),
		PayloadPolicy:  payloadPolicyFunc(denyPayloads("project.secret")),
		SnapshotPolicy: snapshotPolicyFunc(allowPayloads("project.snapshot")),
		CodecRegistry:  codecRegistry{"project.secret": {1: true}},
	})

	if _, err := core.Append(ctx, synclog.AppendRequest{
		StreamID:       streamID,
		PayloadType:    "project.secret",
		PayloadVersion: 1,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	_, err := engine.GatewayCatchUp(ctx, "user:1", &synclogv1.GatewayCatchUpRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if !errors.Is(err, gateway.ErrAccessDenied) {
		t.Fatalf("catch up err = %v, want denied", err)
	}
}

func TestEngineAckEnforcesSubscriberOwnership(t *testing.T) {
	ctx := context.Background()
	target := pbTarget("project", "777", "default")
	streamID := synclog.StreamID("stream:project:777")
	store, core := newCore(t)
	engine := newGateway(t, core, store, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(resolveOnly("user:1", "sub:1")),
		Resolver: &resolver{bindings: map[string][]gateway.StreamBinding{
			targetKey(target): {{StreamID: streamID}},
		}},
		Authorizer:     authorizerFunc(allowOnly("user:1", "sub:1")),
		PayloadPolicy:  payloadPolicyFunc(allowPayloads("project.event")),
		SnapshotPolicy: snapshotPolicyFunc(allowPayloads("project.snapshot")),
		CodecRegistry:  codecRegistry{"project.event": {1: true}},
	})

	if _, err := core.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
		t.Fatalf("append: %v", err)
	}
	_, err := engine.GatewayAck(ctx, "user:1", &synclogv1.GatewayAckRequest{
		SubscriberId: "sub:2",
		Target:       target,
		Seq:          1,
	})
	if !errors.Is(err, gateway.ErrAccessDenied) {
		t.Fatalf("ack err = %v, want denied", err)
	}

	cursor, err := core.GetCursor(ctx, "sub:2", streamID)
	if err != nil {
		t.Fatalf("get cursor: %v", err)
	}
	if cursor.Cursor.Seq != 0 {
		t.Fatalf("unauthorized ack advanced cursor to %d", cursor.Cursor.Seq)
	}
}

func TestEngineTooLongSnapshotRecoveryFlow(t *testing.T) {
	ctx := context.Background()
	target := pbTarget("project", "777", "default")
	streamID := synclog.StreamID("stream:project:777")
	store, core := newCore(t)
	engine := newGateway(t, core, store, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(resolveOnly("user:1", "sub:1")),
		Resolver: &resolver{bindings: map[string][]gateway.StreamBinding{
			targetKey(target): {{
				StreamID:      streamID,
				PayloadTypes:  []string{"project.event"},
				SnapshotTypes: []string{"project.snapshot"},
			}},
		}},
		Authorizer:     authorizerFunc(allowOnly("user:1", "sub:1")),
		PayloadPolicy:  payloadPolicyFunc(allowPayloads("project.event")),
		SnapshotPolicy: snapshotPolicyFunc(allowPayloads("project.snapshot")),
		CodecRegistry:  codecRegistry{"project.event": {1: true}, "project.snapshot": {1: true}},
	})

	for i := 0; i < 5; i++ {
		if _, err := core.Append(ctx, synclog.AppendRequest{
			StreamID:       streamID,
			PayloadType:    "project.event",
			PayloadVersion: 1,
			Payload:        []byte{byte(i)},
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := store.PutSnapshot(ctx, &synclogv1.PutSnapshotRequest{
		StreamId:       string(streamID),
		Seq:            3,
		PayloadType:    "project.snapshot",
		PayloadVersion: 1,
		Checksum:       "sha256:snapshot",
		Payload:        []byte("snapshot-at-3"),
	}); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	if removed, err := store.TruncateBefore(ctx, streamID, 4); err != nil || removed != 3 {
		t.Fatalf("truncate removed %d err %v, want 3 nil", removed, err)
	}

	tooLong, err := engine.GatewayCatchUp(ctx, "user:1", &synclogv1.GatewayCatchUpRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("catch up too long: %v", err)
	}
	if got := tooLong.GetResults()[0].GetStatus(); got != synclogv1.CatchUpStatus_CATCH_UP_STATUS_TOO_LONG {
		t.Fatalf("status = %v, want TOO_LONG", got)
	}

	snapshot, err := engine.GatewayGetLatestSnapshot(ctx, "user:1", &synclogv1.GatewayGetLatestSnapshotRequest{
		SubscriberId: "sub:1",
		Target:       target,
		PayloadType:  "project.snapshot",
	})
	if err != nil {
		t.Fatalf("get latest snapshot: %v", err)
	}
	if snapshot.GetSnapshot().GetSeq() != 3 || string(snapshot.GetSnapshot().GetPayload()) != "snapshot-at-3" {
		t.Fatalf("unexpected snapshot: %+v", snapshot.GetSnapshot())
	}

	if _, err := engine.GatewayAck(ctx, "user:1", &synclogv1.GatewayAckRequest{
		SubscriberId: "sub:1",
		Target:       target,
		Seq:          snapshot.GetSnapshot().GetSeq(),
	}); err != nil {
		t.Fatalf("ack snapshot seq: %v", err)
	}

	recovered, err := engine.GatewayCatchUp(ctx, "user:1", &synclogv1.GatewayCatchUpRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("catch up after snapshot ack: %v", err)
	}
	result := recovered.GetResults()[0]
	if result.GetStatus() != synclogv1.CatchUpStatus_CATCH_UP_STATUS_OK {
		t.Fatalf("status = %v, want OK", result.GetStatus())
	}
	if len(result.GetBatch().GetEvents()) != 2 || result.GetBatch().GetSeqStart() != 4 || result.GetBatch().GetSeq() != 5 {
		t.Fatalf("unexpected recovered batch: %+v", result.GetBatch())
	}
}

func TestEngineGatewaySubscribePollUsesSubscribeAccess(t *testing.T) {
	ctx := context.Background()
	target := pbTarget("project", "777", "default")
	streamID := synclog.StreamID("stream:project:777")
	store, core := newCore(t)
	engine := newGateway(t, core, store, gateway.Hooks{
		SubscriberResolver: subscriberResolverFunc(resolveOnly("user:1", "sub:1")),
		Resolver: &resolver{bindings: map[string][]gateway.StreamBinding{
			targetKey(target): {{StreamID: streamID, PayloadTypes: []string{"project.event"}}},
		}},
		Authorizer: authorizerFunc(func(_ context.Context, req gateway.AuthRequest) error {
			if req.Operation != gateway.OperationSubscribe {
				return errDenied
			}
			return allowOnly("user:1", "sub:1")(ctx, req)
		}),
		PayloadPolicy:  payloadPolicyFunc(allowPayloads("project.event")),
		SnapshotPolicy: snapshotPolicyFunc(allowPayloads("project.snapshot")),
		CodecRegistry:  codecRegistry{"project.event": {1: true}},
	})

	if _, err := core.Append(ctx, synclog.AppendRequest{
		StreamID:       streamID,
		PayloadType:    "project.event",
		PayloadVersion: 1,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	responses, err := engine.GatewaySubscribePoll(ctx, "user:1", &synclogv1.GatewaySubscribeRequest{
		SubscriberId: "sub:1",
		Targets:      []*synclogv1.SyncTarget{target},
	})
	if err != nil {
		t.Fatalf("subscribe poll: %v", err)
	}
	if len(responses) != 1 || len(responses[0].GetBatch().GetEvents()) != 1 {
		t.Fatalf("unexpected responses: %+v", responses)
	}
}

func newCore(t *testing.T) (*memory.Store, *synclog.Engine) {
	t.Helper()
	store := memory.NewStore()
	core, err := synclog.NewEngine(store, store)
	if err != nil {
		t.Fatalf("new core: %v", err)
	}
	return store, core
}

func newGateway(t *testing.T, core *synclog.Engine, snapshots synclog.SnapshotStore, hooks gateway.Hooks) *gateway.Engine {
	t.Helper()
	engine, err := gateway.NewEngine(core, hooks, gateway.WithSnapshotStore(snapshots))
	if err != nil {
		t.Fatalf("new gateway: %v", err)
	}
	return engine
}

type resolver struct {
	bindings map[string][]gateway.StreamBinding
}

func (r *resolver) ResolveTarget(_ context.Context, _ any, target *synclogv1.SyncTarget) (gateway.TargetResolution, error) {
	return gateway.TargetResolution{Target: target, Bindings: r.bindings[targetKey(target)]}, nil
}

type subscriberResolverFunc func(context.Context, any, synclog.SubscriberID) (synclog.SubscriberID, error)

func (f subscriberResolverFunc) ResolveSubscriber(ctx context.Context, actor any, requested synclog.SubscriberID) (synclog.SubscriberID, error) {
	return f(ctx, actor, requested)
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

type codecRegistry map[string]map[uint32]bool

func (r codecRegistry) CanDecode(payloadType string, payloadVersion uint32) bool {
	return r[payloadType][payloadVersion]
}

func resolveOnly(actor any, subscriberID synclog.SubscriberID) func(context.Context, any, synclog.SubscriberID) (synclog.SubscriberID, error) {
	return func(_ context.Context, gotActor any, requested synclog.SubscriberID) (synclog.SubscriberID, error) {
		if gotActor != actor || requested != subscriberID {
			return "", errDenied
		}
		return subscriberID, nil
	}
}

func allowOnly(actor any, subscriberID synclog.SubscriberID) func(context.Context, gateway.AuthRequest) error {
	return func(_ context.Context, req gateway.AuthRequest) error {
		if req.Actor != actor || req.SubscriberID != subscriberID {
			return errDenied
		}
		return nil
	}
}

func allowPayloads(allowed ...string) func(context.Context, any, *synclogv1.SyncTarget, string) error {
	allowedSet := make(map[string]bool, len(allowed))
	for _, payloadType := range allowed {
		allowedSet[payloadType] = true
	}
	return func(_ context.Context, _ any, _ *synclogv1.SyncTarget, payloadType string) error {
		if !allowedSet[payloadType] {
			return errDenied
		}
		return nil
	}
}

func denyPayloads(denied ...string) func(context.Context, any, *synclogv1.SyncTarget, string) error {
	deniedSet := make(map[string]bool, len(denied))
	for _, payloadType := range denied {
		deniedSet[payloadType] = true
	}
	return func(_ context.Context, _ any, _ *synclogv1.SyncTarget, payloadType string) error {
		if deniedSet[payloadType] {
			return errDenied
		}
		return nil
	}
}

func pbTarget(namespace, id, view string) *synclogv1.SyncTarget {
	return &synclogv1.SyncTarget{Namespace: namespace, Id: id, View: view}
}

func targetKey(target *synclogv1.SyncTarget) string {
	if target == nil {
		return ""
	}
	return target.GetNamespace() + "/" + target.GetId() + "/" + target.GetView()
}
