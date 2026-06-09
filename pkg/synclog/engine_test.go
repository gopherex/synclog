package synclog_test

import (
	"context"
	"testing"
	"time"

	"github.com/gopherex/synclog/internal/storage/memory"
	"github.com/gopherex/synclog/pkg/synclog"
)

func TestEngineCatchUpAckFlow(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	engine := newTestEngine(t, store)
	streamID := synclog.StreamID("project:777")
	subscriberID := synclog.SubscriberID("user:1")

	for i := 0; i < 3; i++ {
		result, err := engine.Append(ctx, synclog.AppendRequest{
			StreamID:       streamID,
			Payload:        []byte{byte(i)},
			PayloadType:    "project.event",
			PayloadVersion: 1,
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if want := synclog.Seq(i + 1); result.Event.Seq != want {
			t.Fatalf("append seq = %d, want %d", result.Event.Seq, want)
		}
	}

	first, err := engine.CatchUp(ctx, synclog.CatchUpRequest{
		SubscriberID: subscriberID,
		StreamID:     streamID,
		Limit:        2,
	})
	if err != nil {
		t.Fatalf("catch up: %v", err)
	}
	if first.Status != synclog.CatchUpStatusOK {
		t.Fatalf("status = %v, want OK", first.Status)
	}
	if got := len(first.Batch.Events); got != 2 {
		t.Fatalf("first batch len = %d, want 2", got)
	}
	if first.Batch.SeqStart != 1 || first.Batch.Seq != 2 || first.Batch.Final {
		t.Fatalf("unexpected first batch: %+v", first.Batch)
	}

	ack, err := engine.Ack(ctx, synclog.AckRequest{
		SubscriberID: subscriberID,
		StreamID:     streamID,
		Seq:          first.Batch.Seq,
	})
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if ack.Cursor.Cursor.Seq != 2 {
		t.Fatalf("ack cursor = %d, want 2", ack.Cursor.Cursor.Seq)
	}

	second, err := engine.CatchUp(ctx, synclog.CatchUpRequest{
		SubscriberID: subscriberID,
		StreamID:     streamID,
		Limit:        100,
	})
	if err != nil {
		t.Fatalf("second catch up: %v", err)
	}
	if got := len(second.Batch.Events); got != 1 {
		t.Fatalf("second batch len = %d, want 1", got)
	}
	if second.Batch.Seq != 3 || !second.Batch.Final {
		t.Fatalf("unexpected second batch: %+v", second.Batch)
	}
}

func TestEngineAckIsMonotonic(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	engine := newTestEngine(t, store)
	streamID := synclog.StreamID("chat:123")
	subscriberID := synclog.SubscriberID("user:1")

	for i := 0; i < 2; i++ {
		if _, err := engine.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := engine.Ack(ctx, synclog.AckRequest{SubscriberID: subscriberID, StreamID: streamID, Seq: 2}); err != nil {
		t.Fatalf("ack 2: %v", err)
	}
	ack, err := engine.Ack(ctx, synclog.AckRequest{SubscriberID: subscriberID, StreamID: streamID, Seq: 1})
	if err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	if ack.Cursor.Cursor.Seq != 2 {
		t.Fatalf("cursor moved backwards to %d", ack.Cursor.Cursor.Seq)
	}
}

func TestEngineTooLongAndSnapshotAckRecovery(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	engine := newTestEngine(t, store)
	streamID := synclog.StreamID("task:42")
	subscriberID := synclog.SubscriberID("user:1")

	for i := 0; i < 5; i++ {
		if _, err := engine.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if removed, err := store.TruncateBefore(ctx, streamID, 4); err != nil || removed != 3 {
		t.Fatalf("truncate removed %d err %v, want 3 nil", removed, err)
	}

	tooLong, err := engine.CatchUp(ctx, synclog.CatchUpRequest{
		SubscriberID: subscriberID,
		StreamID:     streamID,
	})
	if err != nil {
		t.Fatalf("catch up too long: %v", err)
	}
	if tooLong.Status != synclog.CatchUpStatusTooLong {
		t.Fatalf("status = %v, want TooLong", tooLong.Status)
	}

	if _, err := engine.Ack(ctx, synclog.AckRequest{
		SubscriberID: subscriberID,
		StreamID:     streamID,
		Seq:          3,
	}); err != nil {
		t.Fatalf("ack snapshot seq: %v", err)
	}
	recovered, err := engine.CatchUp(ctx, synclog.CatchUpRequest{
		SubscriberID: subscriberID,
		StreamID:     streamID,
	})
	if err != nil {
		t.Fatalf("catch up after snapshot ack: %v", err)
	}
	if recovered.Status != synclog.CatchUpStatusOK {
		t.Fatalf("status = %v, want OK", recovered.Status)
	}
	if got := len(recovered.Batch.Events); got != 2 {
		t.Fatalf("recovered batch len = %d, want 2", got)
	}
	if recovered.Batch.SeqStart != 4 || recovered.Batch.Seq != 5 {
		t.Fatalf("unexpected recovered batch: %+v", recovered.Batch)
	}
}

func TestMemoryStoreIdempotency(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	engine := newTestEngine(t, store)
	streamID := synclog.StreamID("project:777")

	first, err := engine.Append(ctx, synclog.AppendRequest{
		StreamID:       streamID,
		IdempotencyKey: "event-1",
		Payload:        []byte("first"),
	})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	second, err := engine.Append(ctx, synclog.AppendRequest{
		StreamID:       streamID,
		IdempotencyKey: "event-1",
		Payload:        []byte("second"),
	})
	if err != nil {
		t.Fatalf("second append: %v", err)
	}
	if !second.Deduplicated {
		t.Fatal("second append was not deduplicated")
	}
	if second.Event.Seq != first.Event.Seq {
		t.Fatalf("deduplicated seq = %d, want %d", second.Event.Seq, first.Event.Seq)
	}
	if string(second.Event.Payload) != "first" {
		t.Fatalf("deduplicated payload = %q, want first", string(second.Event.Payload))
	}
}

func newTestEngine(t *testing.T, store *memory.Store) *synclog.Engine {
	t.Helper()
	engine, err := synclog.NewEngine(
		store,
		store,
		synclog.WithClock(func() time.Time {
			return time.Unix(1_700_000_000, 0)
		}),
	)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine
}
