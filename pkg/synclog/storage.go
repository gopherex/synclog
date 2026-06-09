package synclog

import (
	"context"

	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
)

type EventLog interface {
	// Append assigns the next per-stream seq and persists the event.
	//
	// Idempotency is key-only: if req.IdempotencyKey is non-empty and a prior
	// event with the same key exists on the stream, the adapter MUST return that
	// original event with Deduplicated=true and MUST NOT inspect or compare the
	// new payload/type/version. "First write wins"; a retry with the same key but
	// different content silently resolves to the original event.
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	GetHead(ctx context.Context, streamID StreamID) (StreamHead, error)
	// Read returns events with Seq strictly greater than after, up to limit,
	// contiguous and ordered. Adapters must return ErrTooLong when after is below
	// the retention boundary (see ReplayTooOld). Batch conventions:
	//   - unknown/empty stream: EventBatch{Final: true} (SeqStart=Seq=0).
	//   - caught up (no events after the cursor): EventBatch{Seq: head, Final: true}.
	//   - otherwise: SeqStart/Seq bound the returned events and
	//     Final == (last returned seq == head).
	Read(ctx context.Context, streamID StreamID, after Seq, limit int) (EventBatch, StreamHead, error)
}

type CursorStore interface {
	GetCursor(ctx context.Context, subscriberID SubscriberID, streamID StreamID) (SubscriberCursor, error)
	Ack(ctx context.Context, req AckRequest) (SubscriberCursor, error)
}

type StreamWatcher interface {
	// WaitForStream blocks until the stream head advances past after, the context
	// is done, or an adapter-defined wake-up fires. It MUST return nil only when
	// new events at seq > after may be available; spurious wake-ups for unrelated
	// mutations (acks, cursor resets, retention changes) are a contract violation
	// because they cause subscribe transports to busy-loop.
	WaitForStream(ctx context.Context, streamID StreamID, after Seq) error
}

type CursorAdmin interface {
	ResetCursor(ctx context.Context, req *synclogv1.ResetCursorRequest) (*synclogv1.SubscriberCursor, error)
	ListCursors(ctx context.Context, req *synclogv1.ListCursorsRequest) (*synclogv1.ListCursorsResponse, error)
}

type SnapshotStore interface {
	PutSnapshot(ctx context.Context, req *synclogv1.PutSnapshotRequest) (*synclogv1.PutSnapshotResponse, error)
	GetLatestSnapshot(ctx context.Context, req *synclogv1.GetLatestSnapshotRequest) (*synclogv1.GetLatestSnapshotResponse, error)
	GetSnapshot(ctx context.Context, req *synclogv1.GetSnapshotRequest) (*synclogv1.GetSnapshotResponse, error)
}

type SnapshotAdmin interface {
	ListSnapshots(ctx context.Context, req *synclogv1.ListSnapshotsRequest) (*synclogv1.ListSnapshotsResponse, error)
	DeleteSnapshot(ctx context.Context, req *synclogv1.DeleteSnapshotRequest) (*synclogv1.DeleteSnapshotResponse, error)
}

type StreamRegistry interface {
	CreateStream(ctx context.Context, req *synclogv1.CreateStreamRequest) (*synclogv1.CreateStreamResponse, error)
	DeleteStream(ctx context.Context, req *synclogv1.DeleteStreamRequest) (*synclogv1.DeleteStreamResponse, error)
	ListStreams(ctx context.Context, req *synclogv1.ListStreamsRequest) (*synclogv1.ListStreamsResponse, error)
}

type StreamAdmin interface {
	TruncateStream(ctx context.Context, req *synclogv1.TruncateStreamRequest) (*synclogv1.TruncateStreamResponse, error)
	CompactStream(ctx context.Context, req *synclogv1.CompactStreamRequest) (*synclogv1.CompactStreamResponse, error)
	SetStreamRetention(ctx context.Context, req *synclogv1.SetStreamRetentionRequest) (*synclogv1.SetStreamRetentionResponse, error)
	GetStreamStats(ctx context.Context, req *synclogv1.GetStreamStatsRequest) (*synclogv1.GetStreamStatsResponse, error)
}
