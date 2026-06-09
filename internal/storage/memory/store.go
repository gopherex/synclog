package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
)

type Store struct {
	mu        sync.Mutex
	streams   map[synclog.StreamID]*streamState
	cursors   map[cursorKey]synclog.SubscriberCursor
	snapshots map[snapshotKey]*synclogv1.Snapshot
	retention map[synclog.StreamID]*synclogv1.StreamRetention
	watchers  map[synclog.StreamID][]streamWatcher
}

// streamWatcher is a single WaitForStream subscription. It only wakes when the
// stream head advances past after, so unrelated mutations (acks, cursor resets,
// retention changes) never produce spurious wake-ups.
type streamWatcher struct {
	after synclog.Seq
	ch    chan struct{}
}

type streamState struct {
	head             synclog.Seq
	retainedSeqStart synclog.Seq
	events           []synclog.Event
	idempotency      map[string]synclog.Seq
}

type cursorKey struct {
	subscriberID synclog.SubscriberID
	streamID     synclog.StreamID
}

type snapshotKey struct {
	streamID       synclog.StreamID
	seq            synclog.Seq
	payloadType    string
	payloadVersion uint32
}

func NewStore() *Store {
	return &Store{
		streams:   make(map[synclog.StreamID]*streamState),
		cursors:   make(map[cursorKey]synclog.SubscriberCursor),
		snapshots: make(map[snapshotKey]*synclogv1.Snapshot),
		retention: make(map[synclog.StreamID]*synclogv1.StreamRetention),
		watchers:  make(map[synclog.StreamID][]streamWatcher),
	}
}

func (s *Store) Append(_ context.Context, req synclog.AppendRequest) (synclog.AppendResult, error) {
	if req.StreamID == "" {
		return synclog.AppendResult{}, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.ensureStream(req.StreamID)
	if req.IdempotencyKey != "" {
		if seq, ok := stream.idempotency[req.IdempotencyKey]; ok {
			event, found := stream.eventBySeq(seq)
			if !found {
				return synclog.AppendResult{}, fmt.Errorf("%w: idempotent event was compacted", synclog.ErrTooLong)
			}
			return synclog.AppendResult{Event: cloneEvent(event), Deduplicated: true}, nil
		}
	}

	stream.head++
	event := synclog.Event{
		StreamID:        req.StreamID,
		Seq:             stream.head,
		Payload:         cloneBytes(req.Payload),
		PayloadType:     req.PayloadType,
		PayloadVersion:  req.PayloadVersion,
		IdempotencyKey:  req.IdempotencyKey,
		CreatedAtUnixMS: req.CreatedAtUnixMS,
	}
	stream.events = append(stream.events, event)
	if stream.retainedSeqStart == 0 || event.Seq < stream.retainedSeqStart {
		stream.retainedSeqStart = event.Seq
	}
	if req.IdempotencyKey != "" {
		stream.idempotency[req.IdempotencyKey] = event.Seq
	}
	s.applyRetentionLocked(req.StreamID)
	s.notifyStreamLocked(req.StreamID, stream.head)
	return synclog.AppendResult{Event: cloneEvent(event)}, nil
}

func (s *Store) WaitForStream(ctx context.Context, streamID synclog.StreamID, after synclog.Seq) error {
	if streamID == "" {
		return fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	stream := s.streams[streamID]
	if stream != nil && stream.head > after {
		s.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	s.watchers[streamID] = append(s.watchers[streamID], streamWatcher{after: after, ch: ch})
	s.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		s.removeWatcherLocked(streamID, ch)
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *Store) GetHead(_ context.Context, streamID synclog.StreamID) (synclog.StreamHead, error) {
	if streamID == "" {
		return synclog.StreamHead{}, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.streams[streamID]
	if stream == nil {
		return synclog.StreamHead{Cursor: synclog.Cursor{StreamID: streamID}}, nil
	}
	return stream.headCursor(streamID), nil
}

func (s *Store) Read(_ context.Context, streamID synclog.StreamID, after synclog.Seq, limit int) (synclog.EventBatch, synclog.StreamHead, error) {
	if streamID == "" {
		return synclog.EventBatch{}, synclog.StreamHead{}, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}
	limit = normalizeLimit(limit)

	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.streams[streamID]
	if stream == nil {
		head := synclog.StreamHead{Cursor: synclog.Cursor{StreamID: streamID}}
		return synclog.EventBatch{Final: true}, head, nil
	}

	head := stream.headCursor(streamID)
	if synclog.ReplayTooOld(after, head.RetainedSeqStart) {
		return synclog.EventBatch{}, head, synclog.ErrTooLong
	}

	start := -1
	for i, event := range stream.events {
		if event.Seq > after {
			start = i
			break
		}
	}
	if start == -1 {
		return synclog.EventBatch{Seq: head.Cursor.Seq, Final: true}, head, nil
	}

	end := start + limit
	if end > len(stream.events) {
		end = len(stream.events)
	}

	events := cloneEvents(stream.events[start:end])
	batch := synclog.EventBatch{
		Events:   events,
		SeqStart: events[0].Seq,
		Seq:      events[len(events)-1].Seq,
		Final:    events[len(events)-1].Seq == head.Cursor.Seq,
	}
	return batch, head, nil
}

func (s *Store) GetCursor(_ context.Context, subscriberID synclog.SubscriberID, streamID synclog.StreamID) (synclog.SubscriberCursor, error) {
	if subscriberID == "" {
		return synclog.SubscriberCursor{}, fmt.Errorf("%w: subscriber id is required", synclog.ErrInvalidArgument)
	}
	if streamID == "" {
		return synclog.SubscriberCursor{}, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := cursorKey{subscriberID: subscriberID, streamID: streamID}
	cursor, ok := s.cursors[key]
	if ok {
		return cursor, nil
	}
	return synclog.SubscriberCursor{
		SubscriberID: subscriberID,
		Cursor:       synclog.Cursor{StreamID: streamID},
	}, nil
}

func (s *Store) Ack(_ context.Context, req synclog.AckRequest) (synclog.SubscriberCursor, error) {
	if req.SubscriberID == "" {
		return synclog.SubscriberCursor{}, fmt.Errorf("%w: subscriber id is required", synclog.ErrInvalidArgument)
	}
	if req.StreamID == "" {
		return synclog.SubscriberCursor{}, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := cursorKey{subscriberID: req.SubscriberID, streamID: req.StreamID}
	cursor := s.cursors[key]
	if cursor.SubscriberID == "" {
		cursor = synclog.SubscriberCursor{
			SubscriberID: req.SubscriberID,
			Cursor:       synclog.Cursor{StreamID: req.StreamID},
		}
	}
	if req.Seq > cursor.Cursor.Seq {
		cursor.Cursor.Seq = req.Seq
		cursor.Metadata = req.Metadata
		s.cursors[key] = cursor
	}
	return cursor, nil
}

func (s *Store) CreateStream(_ context.Context, req *synclogv1.CreateStreamRequest) (*synclogv1.CreateStreamResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: create stream request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStream(synclog.StreamID(req.GetStreamId()))
	return &synclogv1.CreateStreamResponse{Stream: &synclogv1.Stream{Id: req.GetStreamId()}}, nil
}

func (s *Store) DeleteStream(_ context.Context, req *synclogv1.DeleteStreamRequest) (*synclogv1.DeleteStreamResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: delete stream request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	streamID := synclog.StreamID(req.GetStreamId())
	_, existed := s.streams[streamID]
	delete(s.streams, streamID)
	delete(s.retention, streamID)
	for key := range s.cursors {
		if key.streamID == streamID {
			delete(s.cursors, key)
		}
	}
	for key := range s.snapshots {
		if key.streamID == streamID {
			delete(s.snapshots, key)
		}
	}
	return &synclogv1.DeleteStreamResponse{Deleted: existed}, nil
}

func (s *Store) ListStreams(_ context.Context, req *synclogv1.ListStreamsRequest) (*synclogv1.ListStreamsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: list streams request is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	limit := int(req.GetLimit())
	if limit <= 0 || limit > synclog.MaxLimit {
		limit = synclog.MaxLimit
	}
	streams := make([]*synclogv1.Stream, 0)
	for streamID := range s.streams {
		if req.GetPrefix() != "" && !hasPrefix(string(streamID), req.GetPrefix()) {
			continue
		}
		streams = append(streams, &synclogv1.Stream{Id: string(streamID)})
		if len(streams) >= limit {
			break
		}
	}
	return &synclogv1.ListStreamsResponse{Streams: streams}, nil
}

func (s *Store) ResetCursor(_ context.Context, req *synclogv1.ResetCursorRequest) (*synclogv1.SubscriberCursor, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: reset cursor request is required", synclog.ErrInvalidArgument)
	}
	if req.GetSubscriberId() == "" {
		return nil, fmt.Errorf("%w: subscriber id is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cursor := synclog.SubscriberCursor{
		SubscriberID:    synclog.SubscriberID(req.GetSubscriberId()),
		Cursor:          synclog.Cursor{StreamID: synclog.StreamID(req.GetStreamId()), Seq: synclog.Seq(req.GetSeq())},
		Metadata:        req.GetMetadata(),
		UpdatedAtUnixMS: 0,
	}
	s.cursors[cursorKey{subscriberID: cursor.SubscriberID, streamID: cursor.Cursor.StreamID}] = cursor
	return toPBSubscriberCursor(cursor), nil
}

func (s *Store) ListCursors(_ context.Context, req *synclogv1.ListCursorsRequest) (*synclogv1.ListCursorsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: list cursors request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cursors := make([]*synclogv1.SubscriberCursor, 0)
	for key, cursor := range s.cursors {
		if string(key.streamID) == req.GetStreamId() {
			cursors = append(cursors, toPBSubscriberCursor(cursor))
		}
	}
	return &synclogv1.ListCursorsResponse{Cursors: cursors}, nil
}

func (s *Store) TruncateBefore(_ context.Context, streamID synclog.StreamID, before synclog.Seq) (int64, error) {
	if streamID == "" {
		return 0, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stream := s.streams[streamID]
	if stream == nil {
		return 0, nil
	}

	keepFrom := 0
	for keepFrom < len(stream.events) && stream.events[keepFrom].Seq < before {
		keepFrom++
	}
	removed := int64(keepFrom)
	stream.events = stream.events[keepFrom:]
	if len(stream.events) > 0 {
		stream.retainedSeqStart = stream.events[0].Seq
	} else if stream.head > 0 {
		stream.retainedSeqStart = stream.head + 1
	}
	return removed, nil
}

func (s *Store) TruncateStream(ctx context.Context, req *synclogv1.TruncateStreamRequest) (*synclogv1.TruncateStreamResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: truncate stream request is required", synclog.ErrInvalidArgument)
	}
	removed, err := s.TruncateBefore(ctx, synclog.StreamID(req.GetStreamId()), synclog.Seq(req.GetBeforeSeq()))
	if err != nil {
		return nil, err
	}
	return &synclogv1.TruncateStreamResponse{Removed: removed}, nil
}

func (s *Store) CompactStream(ctx context.Context, req *synclogv1.CompactStreamRequest) (*synclogv1.CompactStreamResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: compact stream request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	minSeq := synclog.Seq(0)
	found := false
	for key, cursor := range s.cursors {
		if string(key.streamID) != req.GetStreamId() {
			continue
		}
		if !found || cursor.Cursor.Seq < minSeq {
			minSeq = cursor.Cursor.Seq
			found = true
		}
	}
	s.mu.Unlock()

	if found && minSeq > 0 {
		if _, err := s.TruncateBefore(ctx, synclog.StreamID(req.GetStreamId()), minSeq+1); err != nil {
			return nil, err
		}
	}
	return &synclogv1.CompactStreamResponse{Job: &synclogv1.Job{
		JobId: "memory-compact:" + req.GetStreamId(),
		State: synclogv1.JobState_JOB_STATE_DONE,
	}}, nil
}

func (s *Store) SetStreamRetention(_ context.Context, req *synclogv1.SetStreamRetentionRequest) (*synclogv1.SetStreamRetentionResponse, error) {
	if req == nil || req.GetRetention() == nil {
		return nil, fmt.Errorf("%w: stream retention is required", synclog.ErrInvalidArgument)
	}
	if req.GetRetention().GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	retention := &synclogv1.StreamRetention{
		StreamId:  req.GetRetention().GetStreamId(),
		TtlDays:   req.GetRetention().GetTtlDays(),
		MaxEvents: req.GetRetention().GetMaxEvents(),
	}
	s.retention[synclog.StreamID(retention.GetStreamId())] = retention
	s.applyRetentionLocked(synclog.StreamID(retention.GetStreamId()))
	return &synclogv1.SetStreamRetentionResponse{Retention: cloneRetention(retention)}, nil
}

func (s *Store) GetStreamStats(_ context.Context, req *synclogv1.GetStreamStatsRequest) (*synclogv1.GetStreamStatsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: get stream stats request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	streamID := synclog.StreamID(req.GetStreamId())
	stream := s.streams[streamID]
	stats := &synclogv1.StreamStats{StreamId: req.GetStreamId()}
	if stream != nil {
		stats.EventCount = int64(len(stream.events))
		stats.HeadSeq = uint64(stream.head)
		for _, event := range stream.events {
			stats.SizeBytes += int64(len(event.Payload))
		}
	}
	for key := range s.cursors {
		if key.streamID == streamID {
			stats.SubscriberCount++
		}
	}
	return &synclogv1.GetStreamStatsResponse{Stats: stats}, nil
}

func (s *Store) PutSnapshot(_ context.Context, req *synclogv1.PutSnapshotRequest) (*synclogv1.PutSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: put snapshot request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}
	if req.GetPayloadType() == "" {
		return nil, fmt.Errorf("%w: payload type is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := snapshotKey{
		streamID:       synclog.StreamID(req.GetStreamId()),
		seq:            synclog.Seq(req.GetSeq()),
		payloadType:    req.GetPayloadType(),
		payloadVersion: req.GetPayloadVersion(),
	}
	existing, ok := s.snapshots[key]
	if ok && existing.GetRef().GetChecksum() == req.GetChecksum() {
		return &synclogv1.PutSnapshotResponse{Snapshot: cloneSnapshotRef(existing.GetRef()), Deduplicated: true}, nil
	}

	ref := &synclogv1.SnapshotRef{
		StreamId:        req.GetStreamId(),
		Seq:             req.GetSeq(),
		PayloadType:     req.GetPayloadType(),
		PayloadVersion:  req.GetPayloadVersion(),
		Compression:     req.GetCompression(),
		Checksum:        req.GetChecksum(),
		SizeBytes:       int64(len(req.GetPayload())),
		CreatedAtUnixMs: 0,
		ProducerId:      req.GetProducerId(),
	}
	snapshot := &synclogv1.Snapshot{
		Ref:     ref,
		Payload: cloneBytes(req.GetPayload()),
	}
	s.snapshots[key] = snapshot
	return &synclogv1.PutSnapshotResponse{Snapshot: cloneSnapshotRef(ref)}, nil
}

func (s *Store) GetLatestSnapshot(_ context.Context, req *synclogv1.GetLatestSnapshotRequest) (*synclogv1.GetLatestSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: get latest snapshot request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}
	if req.GetPayloadType() == "" {
		return nil, fmt.Errorf("%w: payload type is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var latest *synclogv1.Snapshot
	found := false
	for key, snapshot := range s.snapshots {
		if string(key.streamID) != req.GetStreamId() || key.payloadType != req.GetPayloadType() {
			continue
		}
		if req.GetMaxPayloadVersion() > 0 && key.payloadVersion > req.GetMaxPayloadVersion() {
			continue
		}
		// Highest seq wins; ties break to the higher payload version so selection
		// is deterministic regardless of map iteration order.
		if !found ||
			snapshot.GetRef().GetSeq() > latest.GetRef().GetSeq() ||
			(snapshot.GetRef().GetSeq() == latest.GetRef().GetSeq() &&
				snapshot.GetRef().GetPayloadVersion() > latest.GetRef().GetPayloadVersion()) {
			latest = snapshot
			found = true
		}
	}
	if !found {
		return nil, synclog.ErrNotFound
	}
	return &synclogv1.GetLatestSnapshotResponse{Snapshot: cloneSnapshot(latest)}, nil
}

func (s *Store) GetSnapshot(_ context.Context, req *synclogv1.GetSnapshotRequest) (*synclogv1.GetSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: get snapshot request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}
	if req.GetPayloadType() == "" {
		return nil, fmt.Errorf("%w: payload type is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := snapshotKey{
		streamID:       synclog.StreamID(req.GetStreamId()),
		seq:            synclog.Seq(req.GetSeq()),
		payloadType:    req.GetPayloadType(),
		payloadVersion: req.GetPayloadVersion(),
	}
	snapshot, ok := s.snapshots[key]
	if !ok {
		return nil, synclog.ErrNotFound
	}
	return &synclogv1.GetSnapshotResponse{Snapshot: cloneSnapshot(snapshot)}, nil
}

func (s *Store) ListSnapshots(_ context.Context, req *synclogv1.ListSnapshotsRequest) (*synclogv1.ListSnapshotsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: list snapshots request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	limit := int(req.GetLimit())
	if limit <= 0 || limit > synclog.MaxLimit {
		limit = synclog.MaxLimit
	}
	snapshots := make([]*synclogv1.SnapshotRef, 0)
	for key, snapshot := range s.snapshots {
		if string(key.streamID) != req.GetStreamId() {
			continue
		}
		if req.GetPayloadType() != "" && key.payloadType != req.GetPayloadType() {
			continue
		}
		snapshots = append(snapshots, cloneSnapshotRef(snapshot.GetRef()))
		if len(snapshots) >= limit {
			break
		}
	}
	return &synclogv1.ListSnapshotsResponse{Snapshots: snapshots}, nil
}

func (s *Store) DeleteSnapshot(_ context.Context, req *synclogv1.DeleteSnapshotRequest) (*synclogv1.DeleteSnapshotResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: delete snapshot request is required", synclog.ErrInvalidArgument)
	}
	if req.GetStreamId() == "" {
		return nil, fmt.Errorf("%w: stream id is required", synclog.ErrInvalidArgument)
	}
	if req.GetPayloadType() == "" {
		return nil, fmt.Errorf("%w: payload type is required", synclog.ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := snapshotKey{
		streamID:       synclog.StreamID(req.GetStreamId()),
		seq:            synclog.Seq(req.GetSeq()),
		payloadType:    req.GetPayloadType(),
		payloadVersion: req.GetPayloadVersion(),
	}
	if _, ok := s.snapshots[key]; !ok {
		return &synclogv1.DeleteSnapshotResponse{}, nil
	}
	delete(s.snapshots, key)
	return &synclogv1.DeleteSnapshotResponse{Deleted: true}, nil
}

func (s *Store) ensureStream(streamID synclog.StreamID) *streamState {
	stream := s.streams[streamID]
	if stream == nil {
		stream = &streamState{idempotency: make(map[string]synclog.Seq)}
		s.streams[streamID] = stream
	}
	return stream
}

// notifyStreamLocked wakes only watchers whose requested after is below the new
// head. Watchers still waiting for a higher seq are retained, so the wake-up is
// never spurious. Only Append (which advances head) calls this.
func (s *Store) notifyStreamLocked(streamID synclog.StreamID, head synclog.Seq) {
	watchers := s.watchers[streamID]
	if len(watchers) == 0 {
		return
	}
	kept := watchers[:0]
	for _, w := range watchers {
		if w.after < head {
			close(w.ch)
			continue
		}
		kept = append(kept, w)
	}
	if len(kept) == 0 {
		delete(s.watchers, streamID)
		return
	}
	s.watchers[streamID] = kept
}

func (s *Store) removeWatcherLocked(streamID synclog.StreamID, ch chan struct{}) {
	watchers := s.watchers[streamID]
	for i, w := range watchers {
		if w.ch == ch {
			watchers[i] = watchers[len(watchers)-1]
			watchers = watchers[:len(watchers)-1]
			break
		}
	}
	if len(watchers) == 0 {
		delete(s.watchers, streamID)
		return
	}
	s.watchers[streamID] = watchers
}

func (s *Store) applyRetentionLocked(streamID synclog.StreamID) {
	stream := s.streams[streamID]
	retention := s.retention[streamID]
	if stream == nil || retention == nil || len(stream.events) == 0 {
		return
	}

	remove := 0
	if retention.GetTtlDays() > 0 {
		cutoff := time.Now().Add(-time.Duration(retention.GetTtlDays()) * 24 * time.Hour)
		cutoffMS := synclog.UnixMS(cutoff)
		for remove < len(stream.events) && stream.events[remove].CreatedAtUnixMS > 0 && stream.events[remove].CreatedAtUnixMS < cutoffMS {
			remove++
		}
	}
	if retention.GetMaxEvents() > 0 {
		overflow := len(stream.events) - int(retention.GetMaxEvents())
		if overflow > remove {
			remove = overflow
		}
	}
	if remove <= 0 {
		return
	}
	if remove > len(stream.events) {
		remove = len(stream.events)
	}
	stream.events = stream.events[remove:]
	if len(stream.events) > 0 {
		stream.retainedSeqStart = stream.events[0].Seq
	} else if stream.head > 0 {
		stream.retainedSeqStart = stream.head + 1
	}
}

func (s *streamState) headCursor(streamID synclog.StreamID) synclog.StreamHead {
	return synclog.StreamHead{
		Cursor: synclog.Cursor{
			StreamID: streamID,
			Seq:      s.head,
		},
		RetainedSeqStart: s.retainedSeqStart,
	}
}

func (s *streamState) eventBySeq(seq synclog.Seq) (synclog.Event, bool) {
	for _, event := range s.events {
		if event.Seq == seq {
			return event, true
		}
	}
	return synclog.Event{}, false
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return synclog.DefaultLimit
	}
	if limit > synclog.MaxLimit {
		return synclog.MaxLimit
	}
	return limit
}

func cloneEvents(events []synclog.Event) []synclog.Event {
	out := make([]synclog.Event, len(events))
	for i, event := range events {
		out[i] = cloneEvent(event)
	}
	return out
}

func cloneEvent(event synclog.Event) synclog.Event {
	event.Payload = cloneBytes(event.Payload)
	return event
}

func cloneSnapshot(snapshot *synclogv1.Snapshot) *synclogv1.Snapshot {
	if snapshot == nil {
		return nil
	}
	return &synclogv1.Snapshot{
		Ref:     cloneSnapshotRef(snapshot.GetRef()),
		Payload: cloneBytes(snapshot.GetPayload()),
	}
}

func cloneSnapshotRef(ref *synclogv1.SnapshotRef) *synclogv1.SnapshotRef {
	if ref == nil {
		return nil
	}
	return &synclogv1.SnapshotRef{
		StreamId:        ref.GetStreamId(),
		Seq:             ref.GetSeq(),
		PayloadType:     ref.GetPayloadType(),
		PayloadVersion:  ref.GetPayloadVersion(),
		Compression:     ref.GetCompression(),
		Checksum:        ref.GetChecksum(),
		SizeBytes:       ref.GetSizeBytes(),
		CreatedAtUnixMs: ref.GetCreatedAtUnixMs(),
		ProducerId:      ref.GetProducerId(),
	}
}

func toPBSubscriberCursor(cursor synclog.SubscriberCursor) *synclogv1.SubscriberCursor {
	return &synclogv1.SubscriberCursor{
		SubscriberId: string(cursor.SubscriberID),
		Cursor: &synclogv1.Cursor{
			StreamId: string(cursor.Cursor.StreamID),
			Seq:      uint64(cursor.Cursor.Seq),
		},
		Metadata:        cursor.Metadata,
		UpdatedAtUnixMs: cursor.UpdatedAtUnixMS,
	}
}

func cloneRetention(retention *synclogv1.StreamRetention) *synclogv1.StreamRetention {
	if retention == nil {
		return nil
	}
	return &synclogv1.StreamRetention{
		StreamId:  retention.GetStreamId(),
		TtlDays:   retention.GetTtlDays(),
		MaxEvents: retention.GetMaxEvents(),
	}
}

func hasPrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
