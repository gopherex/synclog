package syncloggrpc

import (
	"context"
	"testing"

	"github.com/gopherex/synclog/internal/storage/memory"
	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
)

func TestServerCoreSnapshotAndAdminMethods(t *testing.T) {
	ctx := context.Background()
	store := memory.NewStore()
	engine, err := synclog.NewEngine(store, store)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	server, err := NewServer(
		engine,
		WithSnapshotStore(store),
		WithSnapshotAdmin(store),
		WithStreamRegistry(store),
		WithStreamAdmin(store),
		WithCursorAdmin(store),
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	if _, err := server.CreateStream(ctx, &synclogv1.CreateStreamRequest{StreamId: "stream:1"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	appendResp, err := server.Append(ctx, &synclogv1.AppendRequest{
		StreamId:       "stream:1",
		Payload:        []byte("event"),
		PayloadType:    "event",
		PayloadVersion: 1,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if appendResp.GetSeq() != 1 {
		t.Fatalf("seq = %d, want 1", appendResp.GetSeq())
	}

	catchUp, err := server.CatchUp(ctx, &synclogv1.CatchUpRequest{
		SubscriberId: "sub:1",
		StreamId:     "stream:1",
	})
	if err != nil {
		t.Fatalf("catch up: %v", err)
	}
	if catchUp.GetStatus() != synclogv1.CatchUpStatus_CATCH_UP_STATUS_OK || len(catchUp.GetBatch().GetEvents()) != 1 {
		t.Fatalf("unexpected catch up: %+v", catchUp)
	}

	if _, err := server.Ack(ctx, &synclogv1.AckRequest{SubscriberId: "sub:1", StreamId: "stream:1", Seq: 1}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, err := server.PutSnapshot(ctx, &synclogv1.PutSnapshotRequest{
		StreamId:       "stream:1",
		Seq:            1,
		PayloadType:    "snapshot",
		PayloadVersion: 1,
		Payload:        []byte("snapshot"),
		Checksum:       "sha256:test",
	}); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	snapshot, err := server.GetLatestSnapshot(ctx, &synclogv1.GetLatestSnapshotRequest{
		StreamId:    "stream:1",
		PayloadType: "snapshot",
	})
	if err != nil {
		t.Fatalf("get latest snapshot: %v", err)
	}
	if string(snapshot.GetSnapshot().GetPayload()) != "snapshot" {
		t.Fatalf("snapshot payload = %q, want snapshot", string(snapshot.GetSnapshot().GetPayload()))
	}

	stats, err := server.GetStreamStats(ctx, &synclogv1.GetStreamStatsRequest{StreamId: "stream:1"})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.GetStats().GetHeadSeq() != 1 || stats.GetStats().GetSubscriberCount() != 1 {
		t.Fatalf("unexpected stats: %+v", stats.GetStats())
	}
}
