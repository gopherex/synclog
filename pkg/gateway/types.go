package gateway

import (
	"context"

	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
)

type Operation string

const (
	OperationOpen        Operation = "open"
	OperationCatchUp     Operation = "catch_up"
	OperationSubscribe   Operation = "subscribe"
	OperationAck         Operation = "ack"
	OperationGetSnapshot Operation = "get_snapshot"
)

type StreamBinding struct {
	// BindingKey is a product-defined stable key for this binding within a
	// target. It is public to clients, unlike StreamID.
	BindingKey string
	StreamID   synclog.StreamID
	// PayloadTypes is the allow-list product code exposes for this target
	// binding. An empty list means the binding itself does not narrow the
	// product's payload policy.
	PayloadTypes []string
	// SnapshotTypes is the allow-list product code exposes for snapshot
	// recovery. An empty list means the binding itself does not narrow the
	// product's snapshot policy.
	SnapshotTypes []string
}

type TargetResolution struct {
	Target   *synclogv1.SyncTarget
	Bindings []StreamBinding
}

type AuthRequest struct {
	Actor        any
	Operation    Operation
	Target       *synclogv1.SyncTarget
	PayloadType  string
	SubscriberID synclog.SubscriberID
}

type SubscriberResolver interface {
	ResolveSubscriber(ctx context.Context, actor any, requested synclog.SubscriberID) (synclog.SubscriberID, error)
}

type Resolver interface {
	ResolveTarget(ctx context.Context, actor any, target *synclogv1.SyncTarget) (TargetResolution, error)
}

type Authorizer interface {
	Authorize(ctx context.Context, req AuthRequest) error
}

type PayloadExposurePolicy interface {
	AllowPayload(ctx context.Context, actor any, target *synclogv1.SyncTarget, payloadType string) error
}

type SnapshotExposurePolicy interface {
	AllowSnapshot(ctx context.Context, actor any, target *synclogv1.SyncTarget, payloadType string) error
}

type CodecRegistry interface {
	CanDecode(payloadType string, payloadVersion uint32) bool
}

type Hooks struct {
	SubscriberResolver SubscriberResolver
	Resolver           Resolver
	Authorizer         Authorizer
	PayloadPolicy      PayloadExposurePolicy
	SnapshotPolicy     SnapshotExposurePolicy
	CodecRegistry      CodecRegistry
}
