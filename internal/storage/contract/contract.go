package contract

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
)

type Store interface {
	synclog.EventLog
	synclog.CursorStore
	synclog.StreamWatcher
	synclog.SnapshotStore
	synclog.SnapshotAdmin
	synclog.StreamAdmin
	synclog.StreamRegistry
	synclog.CursorAdmin
}

func Run(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()

	t.Run("append assigns monotonic seq and deduplicates idempotency key", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := synclog.StreamID("stream:append")

		first, err := store.Append(ctx, synclog.AppendRequest{
			StreamID:       streamID,
			Payload:        []byte("first"),
			IdempotencyKey: "k1",
		})
		if err != nil {
			t.Fatalf("append first: %v", err)
		}
		second, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID})
		if err != nil {
			t.Fatalf("append second: %v", err)
		}
		dupe, err := store.Append(ctx, synclog.AppendRequest{
			StreamID:       streamID,
			Payload:        []byte("changed"),
			IdempotencyKey: "k1",
		})
		if err != nil {
			t.Fatalf("append duplicate: %v", err)
		}
		if first.Event.Seq != 1 || second.Event.Seq != 2 {
			t.Fatalf("seqs = %d/%d, want 1/2", first.Event.Seq, second.Event.Seq)
		}
		if !dupe.Deduplicated || dupe.Event.Seq != first.Event.Seq || string(dupe.Event.Payload) != "first" {
			t.Fatalf("unexpected duplicate result: %+v", dupe)
		}
	})

	t.Run("read returns contiguous batches and final flag", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := synclog.StreamID("stream:read")

		for i := 0; i < 3; i++ {
			if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		batch, head, err := store.Read(ctx, streamID, 0, 2)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if head.Cursor.Seq != 3 {
			t.Fatalf("head = %d, want 3", head.Cursor.Seq)
		}
		if len(batch.Events) != 2 || batch.SeqStart != 1 || batch.Seq != 2 || batch.Final {
			t.Fatalf("unexpected first batch: %+v", batch)
		}
		batch, _, err = store.Read(ctx, streamID, batch.Seq, 2)
		if err != nil {
			t.Fatalf("read second: %v", err)
		}
		if len(batch.Events) != 1 || batch.SeqStart != 3 || batch.Seq != 3 || !batch.Final {
			t.Fatalf("unexpected second batch: %+v", batch)
		}
	})

	t.Run("ack is monotonic", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := synclog.StreamID("stream:ack")

		if _, err := store.Ack(ctx, synclog.AckRequest{SubscriberID: "sub:1", StreamID: streamID, Seq: 5}); err != nil {
			t.Fatalf("ack 5: %v", err)
		}
		cursor, err := store.Ack(ctx, synclog.AckRequest{SubscriberID: "sub:1", StreamID: streamID, Seq: 3})
		if err != nil {
			t.Fatalf("ack 3: %v", err)
		}
		if cursor.Cursor.Seq != 5 {
			t.Fatalf("returned cursor moved backwards to %d", cursor.Cursor.Seq)
		}
		// The regression must be persisted, not just reflected in the return value.
		stored, err := store.GetCursor(ctx, "sub:1", streamID)
		if err != nil {
			t.Fatalf("get cursor: %v", err)
		}
		if stored.Cursor.Seq != 5 {
			t.Fatalf("persisted cursor = %d after backward ack, want 5", stored.Cursor.Seq)
		}
		// A forward ack must advance.
		forward, err := store.Ack(ctx, synclog.AckRequest{SubscriberID: "sub:1", StreamID: streamID, Seq: 7})
		if err != nil {
			t.Fatalf("ack 7: %v", err)
		}
		if forward.Cursor.Seq != 7 {
			t.Fatalf("forward ack cursor = %d, want 7", forward.Cursor.Seq)
		}
	})

	t.Run("watch stream wakes on append after seq", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		streamID := synclog.StreamID("stream:watch")

		done := make(chan error, 1)
		go func() {
			done <- store.WaitForStream(ctx, streamID, 0)
		}()

		if _, err := store.Append(context.Background(), synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append: %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("wait: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("wait timed out: %v", ctx.Err())
		}
	})

	t.Run("truncate makes old cursor too long", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := synclog.StreamID("stream:truncate")

		for i := 0; i < 5; i++ {
			if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		resp, err := store.TruncateStream(ctx, &synclogv1.TruncateStreamRequest{
			StreamId:  string(streamID),
			BeforeSeq: 4,
		})
		if err != nil {
			t.Fatalf("truncate: %v", err)
		}
		if resp.GetRemoved() != 3 {
			t.Fatalf("removed = %d, want 3", resp.GetRemoved())
		}
		_, _, err = store.Read(ctx, streamID, 0, 10)
		if !errors.Is(err, synclog.ErrTooLong) {
			t.Fatalf("read err = %v, want ErrTooLong", err)
		}
	})

	t.Run("max events retention trims old events", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := "stream:retention"

		if _, err := store.SetStreamRetention(ctx, &synclogv1.SetStreamRetentionRequest{
			Retention: &synclogv1.StreamRetention{
				StreamId:  streamID,
				MaxEvents: 2,
			},
		}); err != nil {
			t.Fatalf("set retention: %v", err)
		}
		for i := 0; i < 3; i++ {
			if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: synclog.StreamID(streamID)}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		head, err := store.GetHead(ctx, synclog.StreamID(streamID))
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if head.Cursor.Seq != 3 || head.RetainedSeqStart != 2 {
			t.Fatalf("head = %+v, want seq 3 retained 2", head)
		}
		_, _, err = store.Read(ctx, synclog.StreamID(streamID), 0, 10)
		if !errors.Is(err, synclog.ErrTooLong) {
			t.Fatalf("read err = %v, want ErrTooLong", err)
		}
	})

	t.Run("ttl retention trims old timestamped events", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := "stream:ttl"

		if _, err := store.SetStreamRetention(ctx, &synclogv1.SetStreamRetentionRequest{
			Retention: &synclogv1.StreamRetention{
				StreamId: streamID,
				TtlDays:  1,
			},
		}); err != nil {
			t.Fatalf("set retention: %v", err)
		}
		if _, err := store.Append(ctx, synclog.AppendRequest{
			StreamID:        synclog.StreamID(streamID),
			CreatedAtUnixMS: synclog.UnixMS(time.Now().Add(-48 * time.Hour)),
		}); err != nil {
			t.Fatalf("append old: %v", err)
		}
		if _, err := store.Append(ctx, synclog.AppendRequest{
			StreamID:        synclog.StreamID(streamID),
			CreatedAtUnixMS: synclog.UnixMS(time.Now()),
		}); err != nil {
			t.Fatalf("append fresh: %v", err)
		}
		head, err := store.GetHead(ctx, synclog.StreamID(streamID))
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if head.Cursor.Seq != 2 || head.RetainedSeqStart != 2 {
			t.Fatalf("head = %+v, want seq 2 retained 2", head)
		}
	})

	t.Run("snapshots are latest compatible and exact lookups are isolated", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := "stream:snapshots"

		for _, req := range []*synclogv1.PutSnapshotRequest{
			{StreamId: streamID, Seq: 2, PayloadType: "state", PayloadVersion: 1, Payload: []byte("v1-s2"), Checksum: "a"},
			{StreamId: streamID, Seq: 4, PayloadType: "state", PayloadVersion: 2, Payload: []byte("v2-s4"), Checksum: "b"},
			{StreamId: streamID, Seq: 3, PayloadType: "other", PayloadVersion: 1, Payload: []byte("other"), Checksum: "c"},
		} {
			if _, err := store.PutSnapshot(ctx, req); err != nil {
				t.Fatalf("put snapshot: %v", err)
			}
		}

		latest, err := store.GetLatestSnapshot(ctx, &synclogv1.GetLatestSnapshotRequest{
			StreamId:          streamID,
			PayloadType:       "state",
			MaxPayloadVersion: 1,
		})
		if err != nil {
			t.Fatalf("latest snapshot: %v", err)
		}
		if latest.GetSnapshot().GetRef().GetSeq() != 2 || string(latest.GetSnapshot().GetPayload()) != "v1-s2" {
			t.Fatalf("unexpected latest compatible: %+v", latest.GetSnapshot())
		}

		exact, err := store.GetSnapshot(ctx, &synclogv1.GetSnapshotRequest{
			StreamId:       streamID,
			Seq:            4,
			PayloadType:    "state",
			PayloadVersion: 2,
		})
		if err != nil {
			t.Fatalf("exact snapshot: %v", err)
		}
		if string(exact.GetSnapshot().GetPayload()) != "v2-s4" {
			t.Fatalf("exact payload = %q, want v2-s4", string(exact.GetSnapshot().GetPayload()))
		}

		list, err := store.ListSnapshots(ctx, &synclogv1.ListSnapshotsRequest{
			StreamId:    streamID,
			PayloadType: "state",
		})
		if err != nil {
			t.Fatalf("list snapshots: %v", err)
		}
		if len(list.GetSnapshots()) != 2 {
			t.Fatalf("listed snapshots = %d, want 2", len(list.GetSnapshots()))
		}
	})

	t.Run("read reports empty and unknown streams as final", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()

		batch, head, err := store.Read(ctx, synclog.StreamID("stream:missing"), 0, 10)
		if err != nil {
			t.Fatalf("read unknown: %v", err)
		}
		if !batch.Final || len(batch.Events) != 0 || head.Cursor.Seq != 0 {
			t.Fatalf("unknown stream batch = %+v head = %+v, want empty final", batch, head)
		}

		streamID := synclog.StreamID("stream:caughtup")
		if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append: %v", err)
		}
		batch, _, err = store.Read(ctx, streamID, 1, 10)
		if err != nil {
			t.Fatalf("read caught up: %v", err)
		}
		if !batch.Final || len(batch.Events) != 0 || batch.Seq != 1 {
			t.Fatalf("caught-up batch = %+v, want empty final at head 1", batch)
		}
	})

	t.Run("too-long boundary is exact at retained_seq_start-1", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := synclog.StreamID("stream:boundary")

		for i := 0; i < 5; i++ {
			if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}
		if _, err := store.TruncateStream(ctx, &synclogv1.TruncateStreamRequest{
			StreamId:  string(streamID),
			BeforeSeq: 4,
		}); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		head, err := store.GetHead(ctx, streamID)
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		retained := head.RetainedSeqStart
		if retained != 4 {
			t.Fatalf("retained = %d, want 4", retained)
		}
		// after == retained-1 is the oldest cursor that can still replay.
		if _, _, err := store.Read(ctx, streamID, retained-1, 10); err != nil {
			t.Fatalf("read at retained-1 = %v, want ok", err)
		}
		// after < retained-1 is too long.
		if _, _, err := store.Read(ctx, streamID, retained-2, 10); !errors.Is(err, synclog.ErrTooLong) {
			t.Fatalf("read at retained-2 = %v, want ErrTooLong", err)
		}
	})

	t.Run("watch returns immediately when head already past after", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		streamID := synclog.StreamID("stream:watch-fast")

		if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := store.WaitForStream(ctx, streamID, 0); err != nil {
			t.Fatalf("wait fast-path: %v", err)
		}
	})

	t.Run("watch does not wake until head passes the requested after", func(t *testing.T) {
		store := newStore(t)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		streamID := synclog.StreamID("stream:watch-filter")

		for i := 0; i < 2; i++ {
			if _, err := store.Append(ctx, synclog.AppendRequest{StreamID: streamID}); err != nil {
				t.Fatalf("append %d: %v", i, err)
			}
		}

		done := make(chan error, 1)
		go func() {
			done <- store.WaitForStream(ctx, streamID, 5)
		}()

		// Advancing the head to 3 must NOT wake a waiter that asked for after=5.
		if _, err := store.Append(context.Background(), synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append below after: %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("waiter woke early (err=%v) on head below requested after", err)
		case <-time.After(100 * time.Millisecond):
		}

		// Advancing past after=5 must wake it.
		for i := 0; i < 3; i++ {
			if _, err := store.Append(context.Background(), synclog.AppendRequest{StreamID: streamID}); err != nil {
				t.Fatalf("append past after: %v", err)
			}
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("wait: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("waiter never woke after head passed requested after: %v", ctx.Err())
		}
	})

	t.Run("watch unblocks and cleans up on context cancel", func(t *testing.T) {
		store := newStore(t)
		streamID := synclog.StreamID("stream:watch-cancel")
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			done <- store.WaitForStream(ctx, streamID, 0)
		}()
		// Give the waiter a moment to register, then cancel.
		time.Sleep(20 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("wait err = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter did not return after cancel")
		}
		// A subsequent append on the same stream must not panic on a stale watcher.
		if _, err := store.Append(context.Background(), synclog.AppendRequest{StreamID: streamID}); err != nil {
			t.Fatalf("append after cancel: %v", err)
		}
	})

	t.Run("concurrent appends assign a contiguous gap-free seq range", func(t *testing.T) {
		store := newStore(t)
		streamID := synclog.StreamID("stream:concurrent")
		const n = 50

		var wg sync.WaitGroup
		seqs := make([]synclog.Seq, n)
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				res, err := store.Append(context.Background(), synclog.AppendRequest{StreamID: streamID})
				errs[i] = err
				if err == nil {
					seqs[i] = res.Event.Seq
				}
			}(i)
		}
		wg.Wait()

		seen := make(map[synclog.Seq]bool, n)
		for i := 0; i < n; i++ {
			if errs[i] != nil {
				t.Fatalf("append %d: %v", i, errs[i])
			}
			if seqs[i] < 1 || seqs[i] > n {
				t.Fatalf("seq %d out of range [1,%d]", seqs[i], n)
			}
			if seen[seqs[i]] {
				t.Fatalf("duplicate seq %d assigned", seqs[i])
			}
			seen[seqs[i]] = true
		}
		if len(seen) != n {
			t.Fatalf("assigned %d distinct seqs, want %d", len(seen), n)
		}
	})

	t.Run("snapshot misses return not found and version filter excludes too-new", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := "stream:snap-miss"

		if _, err := store.GetSnapshot(ctx, &synclogv1.GetSnapshotRequest{
			StreamId: streamID, Seq: 1, PayloadType: "state", PayloadVersion: 1,
		}); !errors.Is(err, synclog.ErrNotFound) {
			t.Fatalf("exact miss err = %v, want ErrNotFound", err)
		}
		if _, err := store.GetLatestSnapshot(ctx, &synclogv1.GetLatestSnapshotRequest{
			StreamId: streamID, PayloadType: "state",
		}); !errors.Is(err, synclog.ErrNotFound) {
			t.Fatalf("latest miss err = %v, want ErrNotFound", err)
		}

		if _, err := store.PutSnapshot(ctx, &synclogv1.PutSnapshotRequest{
			StreamId: streamID, Seq: 9, PayloadType: "state", PayloadVersion: 3, Payload: []byte("v3"), Checksum: "z",
		}); err != nil {
			t.Fatalf("put snapshot: %v", err)
		}
		// MaxPayloadVersion below the only snapshot's version must exclude it.
		if _, err := store.GetLatestSnapshot(ctx, &synclogv1.GetLatestSnapshotRequest{
			StreamId: streamID, PayloadType: "state", MaxPayloadVersion: 2,
		}); !errors.Is(err, synclog.ErrNotFound) {
			t.Fatalf("version-filtered latest err = %v, want ErrNotFound", err)
		}
	})

	t.Run("stream registry creates lists and deletes streams", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := "stream:registry"

		if _, err := store.CreateStream(ctx, &synclogv1.CreateStreamRequest{StreamId: streamID}); err != nil {
			t.Fatalf("create stream: %v", err)
		}
		list, err := store.ListStreams(ctx, &synclogv1.ListStreamsRequest{Prefix: "stream:registry"})
		if err != nil {
			t.Fatalf("list streams: %v", err)
		}
		if len(list.GetStreams()) != 1 || list.GetStreams()[0].GetId() != streamID {
			t.Fatalf("listed streams = %+v, want one %q", list.GetStreams(), streamID)
		}
		del, err := store.DeleteStream(ctx, &synclogv1.DeleteStreamRequest{StreamId: streamID})
		if err != nil {
			t.Fatalf("delete stream: %v", err)
		}
		if !del.GetDeleted() {
			t.Fatalf("delete reported not deleted")
		}
		list, err = store.ListStreams(ctx, &synclogv1.ListStreamsRequest{Prefix: "stream:registry"})
		if err != nil {
			t.Fatalf("list streams after delete: %v", err)
		}
		if len(list.GetStreams()) != 0 {
			t.Fatalf("streams remain after delete: %+v", list.GetStreams())
		}
	})

	t.Run("cursor admin resets and lists cursors by stream", func(t *testing.T) {
		store := newStore(t)
		ctx := context.Background()
		streamID := synclog.StreamID("stream:cursor-admin")

		if _, err := store.Ack(ctx, synclog.AckRequest{SubscriberID: "sub:1", StreamID: streamID, Seq: 4}); err != nil {
			t.Fatalf("ack: %v", err)
		}
		// ResetCursor may move a cursor backwards (admin override), unlike Ack.
		if _, err := store.ResetCursor(ctx, &synclogv1.ResetCursorRequest{
			SubscriberId: "sub:1", StreamId: string(streamID), Seq: 1,
		}); err != nil {
			t.Fatalf("reset cursor: %v", err)
		}
		got, err := store.GetCursor(ctx, "sub:1", streamID)
		if err != nil {
			t.Fatalf("get cursor: %v", err)
		}
		if got.Cursor.Seq != 1 {
			t.Fatalf("cursor seq after reset = %d, want 1", got.Cursor.Seq)
		}
		listed, err := store.ListCursors(ctx, &synclogv1.ListCursorsRequest{StreamId: string(streamID)})
		if err != nil {
			t.Fatalf("list cursors: %v", err)
		}
		if len(listed.GetCursors()) != 1 || listed.GetCursors()[0].GetSubscriberId() != "sub:1" {
			t.Fatalf("listed cursors = %+v, want one sub:1", listed.GetCursors())
		}
	})
}
