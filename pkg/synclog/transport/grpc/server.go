package syncloggrpc

import (
	"context"
	"errors"
	"time"

	synclogv1 "github.com/gopherex/synclog/pkg/proto/synclog/v1"
	"github.com/gopherex/synclog/pkg/synclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	synclogv1.UnimplementedSyncLogServiceServer
	synclogv1.UnimplementedSyncLogSnapshotServiceServer
	synclogv1.UnimplementedSyncLogAdminServiceServer

	engine         *synclog.Engine
	snapshots      synclog.SnapshotStore
	snapshotAdmin  synclog.SnapshotAdmin
	streamRegistry synclog.StreamRegistry
	streamAdmin    synclog.StreamAdmin
	cursorAdmin    synclog.CursorAdmin
	watcher        synclog.StreamWatcher
	pollInterval   time.Duration
	heartbeatEvery time.Duration
}

type ServerOption func(*Server)

func WithSnapshotStore(store synclog.SnapshotStore) ServerOption {
	return func(s *Server) {
		s.snapshots = store
	}
}

func WithSnapshotAdmin(admin synclog.SnapshotAdmin) ServerOption {
	return func(s *Server) {
		s.snapshotAdmin = admin
	}
}

func WithStreamRegistry(registry synclog.StreamRegistry) ServerOption {
	return func(s *Server) {
		s.streamRegistry = registry
	}
}

func WithStreamAdmin(admin synclog.StreamAdmin) ServerOption {
	return func(s *Server) {
		s.streamAdmin = admin
	}
}

func WithCursorAdmin(admin synclog.CursorAdmin) ServerOption {
	return func(s *Server) {
		s.cursorAdmin = admin
	}
}

func WithStreamWatcher(watcher synclog.StreamWatcher) ServerOption {
	return func(s *Server) {
		s.watcher = watcher
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

func NewServer(engine *synclog.Engine, opts ...ServerOption) (*Server, error) {
	if engine == nil {
		return nil, status.Error(codes.InvalidArgument, "synclog engine is required")
	}
	s := &Server{
		engine:         engine,
		pollInterval:   100 * time.Millisecond,
		heartbeatEvery: 15 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func RegisterSyncLogService(registrar grpc.ServiceRegistrar, engine *synclog.Engine, opts ...ServerOption) error {
	server, err := NewServer(engine, opts...)
	if err != nil {
		return err
	}
	synclogv1.RegisterSyncLogServiceServer(registrar, server)
	return nil
}

func RegisterSnapshotService(registrar grpc.ServiceRegistrar, engine *synclog.Engine, opts ...ServerOption) error {
	server, err := NewServer(engine, opts...)
	if err != nil {
		return err
	}
	synclogv1.RegisterSyncLogSnapshotServiceServer(registrar, server)
	return nil
}

func RegisterAdminService(registrar grpc.ServiceRegistrar, engine *synclog.Engine, opts ...ServerOption) error {
	server, err := NewServer(engine, opts...)
	if err != nil {
		return err
	}
	synclogv1.RegisterSyncLogAdminServiceServer(registrar, server)
	return nil
}

func RegisterAll(registrar grpc.ServiceRegistrar, engine *synclog.Engine, opts ...ServerOption) error {
	server, err := NewServer(engine, opts...)
	if err != nil {
		return err
	}
	synclogv1.RegisterSyncLogServiceServer(registrar, server)
	synclogv1.RegisterSyncLogSnapshotServiceServer(registrar, server)
	synclogv1.RegisterSyncLogAdminServiceServer(registrar, server)
	return nil
}

func (s *Server) Append(ctx context.Context, req *synclogv1.AppendRequest) (*synclogv1.AppendResponse, error) {
	result, err := s.engine.Append(ctx, synclog.AppendRequest{
		StreamID:       synclog.StreamID(req.GetStreamId()),
		Payload:        cloneBytes(req.GetPayload()),
		PayloadType:    req.GetPayloadType(),
		PayloadVersion: req.GetPayloadVersion(),
		IdempotencyKey: req.GetIdempotencyKey(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	return &synclogv1.AppendResponse{
		Seq:          uint64(result.Event.Seq),
		Deduplicated: result.Deduplicated,
	}, nil
}

func (s *Server) GetHead(ctx context.Context, req *synclogv1.GetHeadRequest) (*synclogv1.GetHeadResponse, error) {
	head, err := s.engine.GetHead(ctx, synclog.StreamID(req.GetStreamId()))
	if err != nil {
		return nil, toStatusError(err)
	}
	return &synclogv1.GetHeadResponse{
		Head:             toPBCursor(head.Cursor),
		RetainedSeqStart: uint64(head.RetainedSeqStart),
	}, nil
}

func (s *Server) GetCursor(ctx context.Context, req *synclogv1.GetCursorRequest) (*synclogv1.GetCursorResponse, error) {
	cursor, err := s.engine.GetCursor(ctx, synclog.SubscriberID(req.GetSubscriberId()), synclog.StreamID(req.GetStreamId()))
	if err != nil {
		return nil, toStatusError(err)
	}
	head, err := s.engine.GetHead(ctx, synclog.StreamID(req.GetStreamId()))
	if err != nil {
		return nil, toStatusError(err)
	}
	return &synclogv1.GetCursorResponse{
		Cursor: toPBCursor(cursor.Cursor),
		Head:   toPBCursor(head.Cursor),
	}, nil
}

func (s *Server) CatchUp(ctx context.Context, req *synclogv1.CatchUpRequest) (*synclogv1.CatchUpResponse, error) {
	result, err := s.engine.CatchUp(ctx, synclog.CatchUpRequest{
		SubscriberID: synclog.SubscriberID(req.GetSubscriberId()),
		StreamID:     synclog.StreamID(req.GetStreamId()),
		Limit:        int(req.GetLimit()),
		TotalLimit:   int(req.GetTotalLimit()),
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	return toPBCatchUpResponse(result), nil
}

func (s *Server) Subscribe(req *synclogv1.SubscribeRequest, stream grpc.ServerStreamingServer[synclogv1.SubscribeResponse]) error {
	ctx := stream.Context()
	delivered := uint64(0)
	tooLongSent := false
	lastHeartbeat := time.Now()

	for {
		select {
		case <-ctx.Done():
			return toStatusError(ctx.Err())
		default:
		}

		result, err := s.engine.CatchUp(ctx, synclog.CatchUpRequest{
			SubscriberID: synclog.SubscriberID(req.GetSubscriberId()),
			StreamID:     synclog.StreamID(req.GetStreamId()),
			Limit:        int(req.GetBatchLimit()),
		})
		if err != nil {
			return toStatusError(err)
		}
		if uint64(result.Cursor.Seq) >= delivered {
			delivered = uint64(result.Cursor.Seq)
			tooLongSent = false
		}
		if req.GetMaxInFlight() > 0 && delivered > uint64(result.Cursor.Seq) &&
			delivered-uint64(result.Cursor.Seq) >= uint64(req.GetMaxInFlight()) {
			if err := s.waitStream(ctx, synclog.StreamID(req.GetStreamId()), result.Head.Cursor.Seq, lastHeartbeat); err != nil {
				return toStatusError(err)
			}
			continue
		}
		if result.Status == synclog.CatchUpStatusTooLong {
			if !tooLongSent {
				resp := toPBSubscribeResponse(result)
				resp.ServerTimeUnixMs = unixMS(time.Now())
				if err := stream.Send(resp); err != nil {
					return toStatusError(err)
				}
				tooLongSent = true
			}
			if err := s.waitStream(ctx, synclog.StreamID(req.GetStreamId()), result.Head.Cursor.Seq, lastHeartbeat); err != nil {
				return toStatusError(err)
			}
			continue
		}
		if len(result.Batch.Events) > 0 && uint64(result.Batch.Seq) > delivered {
			resp := toPBSubscribeResponse(result)
			resp.ServerTimeUnixMs = unixMS(time.Now())
			if err := stream.Send(resp); err != nil {
				return toStatusError(err)
			}
			delivered = uint64(result.Batch.Seq)
			if !result.Batch.Final {
				continue
			}
			if err := s.waitStream(ctx, synclog.StreamID(req.GetStreamId()), result.Head.Cursor.Seq, lastHeartbeat); err != nil {
				return toStatusError(err)
			}
			continue
		}
		if now := time.Now(); now.Sub(lastHeartbeat) >= s.heartbeatEvery {
			if err := stream.Send(&synclogv1.SubscribeResponse{
				Heartbeat:        true,
				ServerTimeUnixMs: unixMS(now),
			}); err != nil {
				return toStatusError(err)
			}
			lastHeartbeat = now
		}

		if err := s.waitStream(ctx, synclog.StreamID(req.GetStreamId()), result.Head.Cursor.Seq, lastHeartbeat); err != nil {
			return toStatusError(err)
		}
	}
}

func (s *Server) waitStream(ctx context.Context, streamID synclog.StreamID, after synclog.Seq, lastHeartbeat time.Time) error {
	timeout := s.pollInterval
	if s.heartbeatEvery > 0 {
		untilHeartbeat := s.heartbeatEvery - time.Since(lastHeartbeat)
		if untilHeartbeat <= 0 {
			return nil
		}
		if timeout <= 0 || untilHeartbeat < timeout {
			timeout = untilHeartbeat
		}
	}
	if timeout <= 0 {
		timeout = s.pollInterval
	}
	if s.watcher == nil {
		return sleep(ctx, timeout)
	}

	waitCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	if err := s.watcher.WaitForStream(waitCtx, streamID, after); err != nil {
		if timeout > 0 && errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	}
	return nil
}

func (s *Server) Ack(ctx context.Context, req *synclogv1.AckRequest) (*synclogv1.AckResponse, error) {
	result, err := s.engine.Ack(ctx, synclog.AckRequest{
		SubscriberID: synclog.SubscriberID(req.GetSubscriberId()),
		StreamID:     synclog.StreamID(req.GetStreamId()),
		Seq:          synclog.Seq(req.GetSeq()),
		Metadata:     req.GetMetadata(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	return &synclogv1.AckResponse{
		Cursor: toPBCursor(result.Cursor.Cursor),
		Head:   toPBCursor(result.Head.Cursor),
	}, nil
}

func (s *Server) PutSnapshot(ctx context.Context, req *synclogv1.PutSnapshotRequest) (*synclogv1.PutSnapshotResponse, error) {
	if s.snapshots == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot store is not configured")
	}
	resp, err := s.snapshots.PutSnapshot(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) GetLatestSnapshot(ctx context.Context, req *synclogv1.GetLatestSnapshotRequest) (*synclogv1.GetLatestSnapshotResponse, error) {
	if s.snapshots == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot store is not configured")
	}
	resp, err := s.snapshots.GetLatestSnapshot(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) GetSnapshot(ctx context.Context, req *synclogv1.GetSnapshotRequest) (*synclogv1.GetSnapshotResponse, error) {
	if s.snapshots == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot store is not configured")
	}
	resp, err := s.snapshots.GetSnapshot(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) ListSnapshots(ctx context.Context, req *synclogv1.ListSnapshotsRequest) (*synclogv1.ListSnapshotsResponse, error) {
	if s.snapshotAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot admin is not configured")
	}
	resp, err := s.snapshotAdmin.ListSnapshots(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) DeleteSnapshot(ctx context.Context, req *synclogv1.DeleteSnapshotRequest) (*synclogv1.DeleteSnapshotResponse, error) {
	if s.snapshotAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "snapshot admin is not configured")
	}
	resp, err := s.snapshotAdmin.DeleteSnapshot(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) CreateStream(ctx context.Context, req *synclogv1.CreateStreamRequest) (*synclogv1.CreateStreamResponse, error) {
	if s.streamRegistry == nil {
		return nil, status.Error(codes.Unimplemented, "stream registry is not configured")
	}
	resp, err := s.streamRegistry.CreateStream(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) DeleteStream(ctx context.Context, req *synclogv1.DeleteStreamRequest) (*synclogv1.DeleteStreamResponse, error) {
	if s.streamRegistry == nil {
		return nil, status.Error(codes.Unimplemented, "stream registry is not configured")
	}
	resp, err := s.streamRegistry.DeleteStream(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) ListStreams(ctx context.Context, req *synclogv1.ListStreamsRequest) (*synclogv1.ListStreamsResponse, error) {
	if s.streamRegistry == nil {
		return nil, status.Error(codes.Unimplemented, "stream registry is not configured")
	}
	resp, err := s.streamRegistry.ListStreams(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) TruncateStream(ctx context.Context, req *synclogv1.TruncateStreamRequest) (*synclogv1.TruncateStreamResponse, error) {
	if s.streamAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "stream admin is not configured")
	}
	resp, err := s.streamAdmin.TruncateStream(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) CompactStream(ctx context.Context, req *synclogv1.CompactStreamRequest) (*synclogv1.CompactStreamResponse, error) {
	if s.streamAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "stream admin is not configured")
	}
	resp, err := s.streamAdmin.CompactStream(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) SetStreamRetention(ctx context.Context, req *synclogv1.SetStreamRetentionRequest) (*synclogv1.SetStreamRetentionResponse, error) {
	if s.streamAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "stream admin is not configured")
	}
	resp, err := s.streamAdmin.SetStreamRetention(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) ResetCursor(ctx context.Context, req *synclogv1.ResetCursorRequest) (*synclogv1.ResetCursorResponse, error) {
	if s.cursorAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "cursor admin is not configured")
	}
	cursor, err := s.cursorAdmin.ResetCursor(ctx, req)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &synclogv1.ResetCursorResponse{Cursor: cursor}, nil
}

func (s *Server) ListCursors(ctx context.Context, req *synclogv1.ListCursorsRequest) (*synclogv1.ListCursorsResponse, error) {
	if s.cursorAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "cursor admin is not configured")
	}
	resp, err := s.cursorAdmin.ListCursors(ctx, req)
	return resp, toStatusError(err)
}

func (s *Server) GetStreamStats(ctx context.Context, req *synclogv1.GetStreamStatsRequest) (*synclogv1.GetStreamStatsResponse, error) {
	if s.streamAdmin == nil {
		return nil, status.Error(codes.Unimplemented, "stream admin is not configured")
	}
	resp, err := s.streamAdmin.GetStreamStats(ctx, req)
	return resp, toStatusError(err)
}

func toPBCatchUpResponse(result synclog.CatchUpResult) *synclogv1.CatchUpResponse {
	return &synclogv1.CatchUpResponse{
		Status:           toPBCatchUpStatus(result.Status),
		Batch:            toPBEventBatch(result.Batch),
		Cursor:           toPBCursor(result.Cursor),
		Head:             toPBCursor(result.Head.Cursor),
		RetainedSeqStart: uint64(result.RetainedSeqStart),
	}
}

func toPBSubscribeResponse(result synclog.CatchUpResult) *synclogv1.SubscribeResponse {
	return &synclogv1.SubscribeResponse{
		Status:           toPBCatchUpStatus(result.Status),
		Batch:            toPBEventBatch(result.Batch),
		Cursor:           toPBCursor(result.Cursor),
		Head:             toPBCursor(result.Head.Cursor),
		RetainedSeqStart: uint64(result.RetainedSeqStart),
	}
}

func toPBEventBatch(batch synclog.EventBatch) *synclogv1.EventBatch {
	return &synclogv1.EventBatch{
		Events:   toPBEvents(batch.Events),
		SeqStart: uint64(batch.SeqStart),
		Seq:      uint64(batch.Seq),
		Final:    batch.Final,
	}
}

func toPBEvents(events []synclog.Event) []*synclogv1.Event {
	out := make([]*synclogv1.Event, 0, len(events))
	for _, event := range events {
		out = append(out, &synclogv1.Event{
			StreamId:        string(event.StreamID),
			Seq:             uint64(event.Seq),
			Payload:         cloneBytes(event.Payload),
			PayloadType:     event.PayloadType,
			PayloadVersion:  event.PayloadVersion,
			IdempotencyKey:  event.IdempotencyKey,
			CreatedAtUnixMs: event.CreatedAtUnixMS,
		})
	}
	return out
}

func toPBCursor(cursor synclog.Cursor) *synclogv1.Cursor {
	return &synclogv1.Cursor{
		StreamId: string(cursor.StreamID),
		Seq:      uint64(cursor.Seq),
	}
}

func toPBCatchUpStatus(status synclog.CatchUpStatus) synclogv1.CatchUpStatus {
	switch status {
	case synclog.CatchUpStatusOK:
		return synclogv1.CatchUpStatus_CATCH_UP_STATUS_OK
	case synclog.CatchUpStatusTooLong:
		return synclogv1.CatchUpStatus_CATCH_UP_STATUS_TOO_LONG
	default:
		return synclogv1.CatchUpStatus_CATCH_UP_STATUS_NONE
	}
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
	case errors.Is(err, synclog.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, synclog.ErrTooLong):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
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

func unixMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func sleep(ctx context.Context, timeout time.Duration) error {
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
