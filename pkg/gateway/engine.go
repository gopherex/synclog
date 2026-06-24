package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
)

const defaultBindingKey = "default"

type Engine struct {
	core      *synclog.Engine
	snapshots synclog.SnapshotStore
	watcher   synclog.StreamWatcher
	hooks     Hooks
}

type EngineOption func(*Engine)

func WithSnapshotStore(store synclog.SnapshotStore) EngineOption {
	return func(e *Engine) {
		e.snapshots = store
	}
}

func WithStreamWatcher(watcher synclog.StreamWatcher) EngineOption {
	return func(e *Engine) {
		e.watcher = watcher
	}
}

func NewEngine(core *synclog.Engine, hooks Hooks, opts ...EngineOption) (*Engine, error) {
	if core == nil {
		return nil, fmt.Errorf("%w: core engine is required", synclog.ErrInvalidArgument)
	}
	if hooks.SubscriberResolver == nil {
		return nil, fmt.Errorf("%w: subscriber resolver hook is required", synclog.ErrInvalidArgument)
	}
	if hooks.Resolver == nil {
		return nil, fmt.Errorf("%w: resolver hook is required", synclog.ErrInvalidArgument)
	}
	if hooks.Authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer hook is required", synclog.ErrInvalidArgument)
	}
	if hooks.PayloadPolicy == nil {
		return nil, fmt.Errorf("%w: payload exposure policy hook is required", synclog.ErrInvalidArgument)
	}
	if hooks.SnapshotPolicy == nil {
		return nil, fmt.Errorf("%w: snapshot exposure policy hook is required", synclog.ErrInvalidArgument)
	}
	if hooks.CodecRegistry == nil {
		return nil, fmt.Errorf("%w: codec registry hook is required", synclog.ErrInvalidArgument)
	}

	e := &Engine{core: core, hooks: hooks}
	for _, opt := range opts {
		opt(e)
	}
	return e, nil
}

func (e *Engine) Open(ctx context.Context, actor any, req *synclogv1.OpenRequest) (*synclogv1.OpenResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: open request is required", synclog.ErrInvalidArgument)
	}
	subscriberID, err := e.resolveSubscriber(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()))
	if err != nil {
		return nil, err
	}
	if err := validateTargets(req.GetTargets()); err != nil {
		return nil, err
	}

	states := make([]*synclogv1.TargetState, 0, len(req.GetTargets()))
	for _, target := range req.GetTargets() {
		bindings, err := e.resolveBindings(ctx, actor, target)
		if err != nil {
			return nil, err
		}
		if err := e.authorize(ctx, AuthRequest{
			Actor:        actor,
			Operation:    OperationOpen,
			Target:       target,
			SubscriberID: subscriberID,
		}); err != nil {
			return nil, err
		}

		state, err := e.targetState(ctx, subscriberID, target, bindings)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return &synclogv1.OpenResponse{Targets: states}, nil
}

// ResolveSubscribeTargets resolves and authorizes targets for incremental
// addition to a live subscribe stream. It returns the resolved state of each
// accepted target and a per-target rejection for each target that fails
// resolution or authorization. Rejecting one target never fails the call: this
// isolates per-target failures so one bad target cannot tear down the stream.
//
// A nil error means the subscriber itself resolved; only subscriber resolution
// (or argument) failures are returned as a fatal error.
func (e *Engine) ResolveSubscribeTargets(ctx context.Context, actor any, requestedSubscriberID synclog.SubscriberID, targets []*synclogv1.SyncTarget) ([]*synclogv1.TargetState, []*synclogv1.TargetRejection, error) {
	subscriberID, err := e.resolveSubscriber(ctx, actor, requestedSubscriberID)
	if err != nil {
		return nil, nil, err
	}

	states := make([]*synclogv1.TargetState, 0, len(targets))
	rejections := make([]*synclogv1.TargetRejection, 0)
	for _, target := range targets {
		state, err := e.resolveSubscribeTarget(ctx, actor, subscriberID, target)
		if err != nil {
			rejections = append(rejections, &synclogv1.TargetRejection{
				Target:  cloneTarget(target),
				Code:    rejectionCode(err),
				Message: err.Error(),
			})
			continue
		}
		states = append(states, state)
	}
	return states, rejections, nil
}

func (e *Engine) resolveSubscribeTarget(ctx context.Context, actor any, subscriberID synclog.SubscriberID, target *synclogv1.SyncTarget) (*synclogv1.TargetState, error) {
	bindings, err := e.resolveBindings(ctx, actor, target)
	if err != nil {
		return nil, err
	}
	if err := e.authorize(ctx, AuthRequest{
		Actor:        actor,
		Operation:    OperationSubscribe,
		Target:       target,
		SubscriberID: subscriberID,
	}); err != nil {
		return nil, err
	}
	return e.targetState(ctx, subscriberID, target, bindings)
}

func (e *Engine) GatewayCatchUp(ctx context.Context, actor any, req *synclogv1.GatewayCatchUpRequest) (*synclogv1.GatewayCatchUpResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: catch-up request is required", synclog.ErrInvalidArgument)
	}
	return e.catchUp(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()), req.GetTargets(), int(req.GetLimitPerTarget()), int(req.GetTotalLimitPerTarget()), OperationCatchUp)
}

func (e *Engine) GatewaySubscribePoll(ctx context.Context, actor any, req *synclogv1.GatewaySubscribeRequest) ([]*synclogv1.GatewaySubscribeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: subscribe request is required", synclog.ErrInvalidArgument)
	}
	// totalLimit is 0 (no one-shot replay budget) for live subscription: an
	// established subscription should not be aborted by a total-replay cap.
	// Falling too far behind is still surfaced as TOO_LONG via the retention
	// boundary (ReplayTooOld), which is independent of totalLimit.
	catchUp, err := e.catchUp(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()), req.GetTargets(), int(req.GetBatchLimitPerTarget()), 0, OperationSubscribe)
	if err != nil {
		return nil, err
	}
	responses := make([]*synclogv1.GatewaySubscribeResponse, 0, len(catchUp.GetResults()))
	for _, result := range catchUp.GetResults() {
		responses = append(responses, &synclogv1.GatewaySubscribeResponse{
			Status:     result.GetStatus(),
			Batch:      result.GetBatch(),
			State:      result.GetState(),
			BindingKey: result.GetBindingKey(),
		})
	}
	return responses, nil
}

func (e *Engine) WaitGatewaySubscribe(ctx context.Context, actor any, req *synclogv1.GatewaySubscribeRequest, after map[string]uint64, timeout time.Duration) error {
	if req == nil {
		return fmt.Errorf("%w: subscribe request is required", synclog.ErrInvalidArgument)
	}
	subscriberID, err := e.resolveSubscriber(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()))
	if err != nil {
		return err
	}
	if err := validateTargets(req.GetTargets()); err != nil {
		return err
	}
	if e.watcher == nil {
		for _, target := range req.GetTargets() {
			if _, err := e.resolveBindings(ctx, actor, target); err != nil {
				return err
			}
			if err := e.authorize(ctx, AuthRequest{
				Actor:        actor,
				Operation:    OperationSubscribe,
				Target:       target,
				SubscriberID: subscriberID,
			}); err != nil {
				return err
			}
		}
		return waitTimeout(ctx, timeout)
	}

	// Always derive a cancelable context so the deferred cancel tears down every
	// spawned WaitForStream goroutine when this call returns, even when timeout
	// <= 0 (otherwise non-firing watchers leak until the parent ctx is canceled).
	var (
		watchCtx context.Context
		cancel   context.CancelFunc
	)
	if timeout > 0 {
		watchCtx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		watchCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	done := make(chan error, 1)
	watches := 0
	for _, target := range req.GetTargets() {
		bindings, err := e.resolveBindings(ctx, actor, target)
		if err != nil {
			return err
		}
		if err := e.authorize(ctx, AuthRequest{
			Actor:        actor,
			Operation:    OperationSubscribe,
			Target:       target,
			SubscriberID: subscriberID,
		}); err != nil {
			return err
		}
		for _, binding := range bindings {
			key := targetBindingKey(target, binding.BindingKey)
			head, err := e.core.GetHead(ctx, binding.StreamID)
			if err != nil {
				return err
			}
			if uint64(head.Cursor.Seq) > after[key] {
				return nil
			}
			watches++
			go func(streamID synclog.StreamID, afterSeq uint64) {
				if err := e.watcher.WaitForStream(watchCtx, streamID, synclog.Seq(afterSeq)); err != nil {
					select {
					case done <- err:
					default:
					}
					return
				}
				select {
				case done <- nil:
				default:
				}
			}(binding.StreamID, after[key])
		}
	}
	if watches == 0 {
		return fmt.Errorf("%w: at least one target binding is required", synclog.ErrInvalidArgument)
	}

	err = <-done
	if err == nil || ctx.Err() != nil {
		return err
	}
	if timeout > 0 && err == context.DeadlineExceeded {
		return nil
	}
	return err
}

func (e *Engine) catchUp(ctx context.Context, actor any, requestedSubscriberID synclog.SubscriberID, targets []*synclogv1.SyncTarget, limit int, totalLimit int, operation Operation) (*synclogv1.GatewayCatchUpResponse, error) {
	subscriberID, err := e.resolveSubscriber(ctx, actor, requestedSubscriberID)
	if err != nil {
		return nil, err
	}
	if err := validateTargets(targets); err != nil {
		return nil, err
	}

	results := make([]*synclogv1.GatewayCatchUpResult, 0, len(targets))
	for _, target := range targets {
		bindings, err := e.resolveBindings(ctx, actor, target)
		if err != nil {
			return nil, err
		}
		if err := e.authorize(ctx, AuthRequest{
			Actor:        actor,
			Operation:    operation,
			Target:       target,
			SubscriberID: subscriberID,
		}); err != nil {
			return nil, err
		}

		// Compute aggregate target state once per target rather than once per
		// binding: it reads every binding's cursor/head, so doing it inside the
		// per-binding loop is O(N^2) reads and can return inconsistent snapshots.
		state, err := e.targetState(ctx, subscriberID, target, bindings)
		if err != nil {
			return nil, err
		}

		for _, binding := range bindings {
			catchUp, err := e.core.CatchUp(ctx, synclog.CatchUpRequest{
				SubscriberID: subscriberID,
				StreamID:     binding.StreamID,
				Limit:        limit,
				TotalLimit:   totalLimit,
			})
			if err != nil {
				return nil, err
			}

			result := &synclogv1.GatewayCatchUpResult{
				Target:     cloneTarget(target),
				Status:     toPBCatchUpStatus(catchUp.Status),
				State:      state,
				BindingKey: binding.BindingKey,
			}
			if catchUp.Status == synclog.CatchUpStatusOK {
				batch, err := e.gatewayBatch(ctx, actor, subscriberID, target, binding, catchUp.Batch, operation)
				if err != nil {
					return nil, err
				}
				result.Batch = batch
			}
			results = append(results, result)
		}
	}
	return &synclogv1.GatewayCatchUpResponse{Results: results}, nil
}

func (e *Engine) GatewayAck(ctx context.Context, actor any, req *synclogv1.GatewayAckRequest) (*synclogv1.GatewayAckResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: ack request is required", synclog.ErrInvalidArgument)
	}
	subscriberID, err := e.resolveSubscriber(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()))
	if err != nil {
		return nil, err
	}
	if err := validateTarget(req.GetTarget()); err != nil {
		return nil, err
	}

	bindings, err := e.resolveBindings(ctx, actor, req.GetTarget())
	if err != nil {
		return nil, err
	}
	// Authorize before selecting the binding so an unauthorized actor cannot use
	// binding-key validation errors to probe which keys exist on a target.
	if err := e.authorize(ctx, AuthRequest{
		Actor:        actor,
		Operation:    OperationAck,
		Target:       req.GetTarget(),
		SubscriberID: subscriberID,
	}); err != nil {
		return nil, err
	}
	binding, err := selectBinding(bindings, req.GetBindingKey())
	if err != nil {
		return nil, err
	}

	ack, err := e.core.Ack(ctx, synclog.AckRequest{
		SubscriberID: subscriberID,
		StreamID:     binding.StreamID,
		Seq:          synclog.Seq(req.GetSeq()),
		Metadata:     req.GetMetadata(),
	})
	if err != nil {
		return nil, err
	}
	_ = ack
	state, err := e.targetState(ctx, subscriberID, req.GetTarget(), bindings)
	if err != nil {
		return nil, err
	}
	return &synclogv1.GatewayAckResponse{State: state}, nil
}

func (e *Engine) GatewayGetLatestSnapshot(ctx context.Context, actor any, req *synclogv1.GatewayGetLatestSnapshotRequest) (*synclogv1.GatewayGetLatestSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: get latest snapshot request is required", synclog.ErrInvalidArgument)
	}
	if e.snapshots == nil {
		return nil, fmt.Errorf("%w: snapshot store is required", synclog.ErrInvalidArgument)
	}
	subscriberID, err := e.resolveSubscriber(ctx, actor, synclog.SubscriberID(req.GetSubscriberId()))
	if err != nil {
		return nil, err
	}
	if err := validateTarget(req.GetTarget()); err != nil {
		return nil, err
	}
	if req.GetPayloadType() == "" {
		return nil, fmt.Errorf("%w: payload type is required", synclog.ErrInvalidArgument)
	}

	bindings, err := e.resolveBindings(ctx, actor, req.GetTarget())
	if err != nil {
		return nil, err
	}
	// Authorize before selecting the binding so an unauthorized actor cannot use
	// binding-key validation errors to probe which keys exist on a target.
	if err := e.authorize(ctx, AuthRequest{
		Actor:        actor,
		Operation:    OperationGetSnapshot,
		Target:       req.GetTarget(),
		PayloadType:  req.GetPayloadType(),
		SubscriberID: subscriberID,
	}); err != nil {
		return nil, err
	}
	binding, err := selectBinding(bindings, req.GetBindingKey())
	if err != nil {
		return nil, err
	}
	if err := requireAllowedType(binding.SnapshotTypes, req.GetPayloadType(), "snapshot"); err != nil {
		return nil, err
	}
	if err := e.hooks.SnapshotPolicy.AllowSnapshot(ctx, actor, req.GetTarget(), req.GetPayloadType()); err != nil {
		return nil, err
	}

	snapshotResp, err := e.snapshots.GetLatestSnapshot(ctx, &synclogv1.GetLatestSnapshotRequest{
		StreamId:          string(binding.StreamID),
		PayloadType:       req.GetPayloadType(),
		MaxPayloadVersion: req.GetMaxPayloadVersion(),
	})
	if err != nil {
		return nil, err
	}
	snapshot := snapshotResp.GetSnapshot()
	ref := snapshot.GetRef()
	if ref == nil {
		return nil, fmt.Errorf("%w: snapshot ref is required", synclog.ErrInvalidArgument)
	}
	if !e.hooks.CodecRegistry.CanDecode(ref.GetPayloadType(), ref.GetPayloadVersion()) {
		return nil, fmt.Errorf("%w: no codec for snapshot type %q version %d", synclog.ErrInvalidArgument, ref.GetPayloadType(), ref.GetPayloadVersion())
	}

	state, err := e.targetState(ctx, subscriberID, req.GetTarget(), bindings)
	if err != nil {
		return nil, err
	}
	return &synclogv1.GatewayGetLatestSnapshotResponse{
		Snapshot: &synclogv1.GatewaySnapshot{
			Target:         cloneTarget(req.GetTarget()),
			Seq:            ref.GetSeq(),
			Payload:        cloneBytes(snapshot.GetPayload()),
			PayloadType:    ref.GetPayloadType(),
			PayloadVersion: ref.GetPayloadVersion(),
			Compression:    ref.GetCompression(),
			Checksum:       ref.GetChecksum(),
			BindingKey:     binding.BindingKey,
		},
		State: state,
	}, nil
}

func (e *Engine) resolveSubscriber(ctx context.Context, actor any, requested synclog.SubscriberID) (synclog.SubscriberID, error) {
	if requested == "" {
		return "", fmt.Errorf("%w: subscriber id is required", synclog.ErrInvalidArgument)
	}
	subscriberID, err := e.hooks.SubscriberResolver.ResolveSubscriber(ctx, actor, requested)
	if err != nil {
		return "", err
	}
	if subscriberID == "" {
		return "", fmt.Errorf("%w: subscriber resolver returned empty subscriber id", synclog.ErrInvalidArgument)
	}
	return subscriberID, nil
}

func (e *Engine) resolveBindings(ctx context.Context, actor any, target *synclogv1.SyncTarget) ([]StreamBinding, error) {
	if err := validateTarget(target); err != nil {
		return nil, err
	}
	resolution, err := e.hooks.Resolver.ResolveTarget(ctx, actor, target)
	if err != nil {
		return nil, err
	}
	if len(resolution.Bindings) == 0 {
		return nil, fmt.Errorf("%w: target must resolve to at least one stream binding", synclog.ErrInvalidArgument)
	}
	out := make([]StreamBinding, 0, len(resolution.Bindings))
	seen := make(map[string]struct{}, len(resolution.Bindings))
	for _, binding := range resolution.Bindings {
		if binding.StreamID == "" {
			return nil, fmt.Errorf("%w: stream id is required in target binding", synclog.ErrInvalidArgument)
		}
		if binding.BindingKey == "" {
			if len(resolution.Bindings) != 1 {
				return nil, fmt.Errorf("%w: binding key is required for multi-binding target", synclog.ErrInvalidArgument)
			}
			binding.BindingKey = defaultBindingKey
		}
		if _, ok := seen[binding.BindingKey]; ok {
			return nil, fmt.Errorf("%w: duplicate binding key %q", synclog.ErrInvalidArgument, binding.BindingKey)
		}
		seen[binding.BindingKey] = struct{}{}
		out = append(out, binding)
	}
	return out, nil
}

func (e *Engine) authorize(ctx context.Context, req AuthRequest) error {
	return e.hooks.Authorizer.Authorize(ctx, req)
}

func (e *Engine) targetState(ctx context.Context, subscriberID synclog.SubscriberID, target *synclogv1.SyncTarget, bindings []StreamBinding) (*synclogv1.TargetState, error) {
	state := &synclogv1.TargetState{
		Target:   cloneTarget(target),
		Bindings: make([]*synclogv1.TargetBindingState, 0, len(bindings)),
	}
	var cursorSet bool
	var retainedSet bool
	for _, binding := range bindings {
		cursor, err := e.core.GetCursor(ctx, subscriberID, binding.StreamID)
		if err != nil {
			return nil, err
		}
		head, err := e.core.GetHead(ctx, binding.StreamID)
		if err != nil {
			return nil, err
		}
		bindingState := &synclogv1.TargetBindingState{
			BindingKey:       binding.BindingKey,
			CursorSeq:        uint64(cursor.Cursor.Seq),
			HeadSeq:          uint64(head.Cursor.Seq),
			RetainedSeqStart: uint64(head.RetainedSeqStart),
		}
		state.Bindings = append(state.Bindings, bindingState)
		if !cursorSet || bindingState.CursorSeq < state.CursorSeq {
			state.CursorSeq = bindingState.CursorSeq
			cursorSet = true
		}
		if bindingState.HeadSeq > state.HeadSeq {
			state.HeadSeq = bindingState.HeadSeq
		}
		if bindingState.RetainedSeqStart > 0 && (!retainedSet || bindingState.RetainedSeqStart < state.RetainedSeqStart) {
			state.RetainedSeqStart = bindingState.RetainedSeqStart
			retainedSet = true
		}
	}
	return state, nil
}

func (e *Engine) gatewayBatch(ctx context.Context, actor any, subscriberID synclog.SubscriberID, target *synclogv1.SyncTarget, binding StreamBinding, batch synclog.EventBatch, operation Operation) (*synclogv1.GatewayBatch, error) {
	out := &synclogv1.GatewayBatch{
		Target:     cloneTarget(target),
		SeqStart:   uint64(batch.SeqStart),
		Seq:        uint64(batch.Seq),
		Final:      batch.Final,
		BindingKey: binding.BindingKey,
	}
	if len(batch.Events) == 0 {
		return out, nil
	}

	out.Events = make([]*synclogv1.GatewayEvent, 0, len(batch.Events))
	for _, event := range batch.Events {
		if err := requireAllowedType(binding.PayloadTypes, event.PayloadType, "payload"); err != nil {
			return nil, err
		}
		if err := e.authorize(ctx, AuthRequest{
			Actor:        actor,
			Operation:    operation,
			Target:       target,
			PayloadType:  event.PayloadType,
			SubscriberID: subscriberID,
		}); err != nil {
			return nil, err
		}
		if err := e.hooks.PayloadPolicy.AllowPayload(ctx, actor, target, event.PayloadType); err != nil {
			return nil, err
		}
		if !e.hooks.CodecRegistry.CanDecode(event.PayloadType, event.PayloadVersion) {
			return nil, fmt.Errorf("%w: no codec for payload type %q version %d", synclog.ErrInvalidArgument, event.PayloadType, event.PayloadVersion)
		}
		out.Events = append(out.Events, &synclogv1.GatewayEvent{
			Target:          cloneTarget(target),
			Seq:             uint64(event.Seq),
			Payload:         cloneBytes(event.Payload),
			PayloadType:     event.PayloadType,
			PayloadVersion:  event.PayloadVersion,
			CreatedAtUnixMs: event.CreatedAtUnixMS,
			BindingKey:      binding.BindingKey,
		})
	}
	return out, nil
}

func selectBinding(bindings []StreamBinding, requested string) (StreamBinding, error) {
	if requested == "" {
		if len(bindings) == 1 {
			return bindings[0], nil
		}
		return StreamBinding{}, fmt.Errorf("%w: binding key is required for multi-binding target", synclog.ErrInvalidArgument)
	}
	for _, binding := range bindings {
		if binding.BindingKey == requested {
			return binding, nil
		}
	}
	return StreamBinding{}, fmt.Errorf("%w: unknown binding key %q", synclog.ErrInvalidArgument, requested)
}

func validateTargets(targets []*synclogv1.SyncTarget) error {
	if len(targets) == 0 {
		return fmt.Errorf("%w: at least one target is required", synclog.ErrInvalidArgument)
	}
	for _, target := range targets {
		if err := validateTarget(target); err != nil {
			return err
		}
	}
	return nil
}

func validateTarget(target *synclogv1.SyncTarget) error {
	if target == nil {
		return fmt.Errorf("%w: target is required", synclog.ErrInvalidArgument)
	}
	if target.GetNamespace() == "" {
		return fmt.Errorf("%w: target namespace is required", synclog.ErrInvalidArgument)
	}
	if target.GetId() == "" {
		return fmt.Errorf("%w: target id is required", synclog.ErrInvalidArgument)
	}
	return nil
}

func requireAllowedType(allowed []string, payloadType string, kind string) error {
	if len(allowed) == 0 {
		return nil
	}
	for _, candidate := range allowed {
		if candidate == payloadType {
			return nil
		}
	}
	return fmt.Errorf("%w: %s type %q is not allowed by target binding", synclog.ErrInvalidArgument, kind, payloadType)
}

// rejectionCode maps a target resolution/authorization error to a stable,
// gRPC-style code string for TargetRejection. It mirrors the transport's
// error→status mapping but stays transport-agnostic in the engine.
func rejectionCode(err error) string {
	switch {
	case errors.Is(err, ErrAccessDenied):
		return "PERMISSION_DENIED"
	case errors.Is(err, synclog.ErrInvalidArgument):
		return "INVALID_ARGUMENT"
	case errors.Is(err, synclog.ErrNotFound):
		return "NOT_FOUND"
	case errors.Is(err, synclog.ErrTooLong):
		return "FAILED_PRECONDITION"
	default:
		return "INTERNAL"
	}
}

func toPBCatchUpStatus(in synclog.CatchUpStatus) synclogv1.CatchUpStatus {
	switch in {
	case synclog.CatchUpStatusOK:
		return synclogv1.CatchUpStatus_CATCH_UP_STATUS_OK
	case synclog.CatchUpStatusTooLong:
		return synclogv1.CatchUpStatus_CATCH_UP_STATUS_TOO_LONG
	default:
		return synclogv1.CatchUpStatus_CATCH_UP_STATUS_NONE
	}
}

func waitTimeout(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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

func cloneTarget(in *synclogv1.SyncTarget) *synclogv1.SyncTarget {
	if in == nil {
		return nil
	}
	return &synclogv1.SyncTarget{
		Namespace: in.GetNamespace(),
		Id:        in.GetId(),
		View:      in.GetView(),
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
