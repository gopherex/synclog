package synclog

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Clock func() time.Time

type Engine struct {
	log     EventLog
	cursors CursorStore
	clock   Clock
}

type EngineOption func(*Engine)

func WithClock(clock Clock) EngineOption {
	return func(e *Engine) {
		if clock != nil {
			e.clock = clock
		}
	}
}

func NewEngine(log EventLog, cursors CursorStore, opts ...EngineOption) (*Engine, error) {
	if log == nil {
		return nil, fmt.Errorf("%w: event log is required", ErrInvalidArgument)
	}
	if cursors == nil {
		return nil, fmt.Errorf("%w: cursor store is required", ErrInvalidArgument)
	}

	e := &Engine{
		log:     log,
		cursors: cursors,
		clock:   time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e, nil
}

func (e *Engine) Append(ctx context.Context, req AppendRequest) (AppendResult, error) {
	if req.StreamID == "" {
		return AppendResult{}, fmt.Errorf("%w: stream id is required", ErrInvalidArgument)
	}
	if req.CreatedAtUnixMS < 0 {
		return AppendResult{}, fmt.Errorf("%w: created_at must not be negative", ErrInvalidArgument)
	}
	if req.CreatedAtUnixMS == 0 {
		req.CreatedAtUnixMS = UnixMS(e.clock())
	}
	return e.log.Append(ctx, req)
}

func (e *Engine) GetHead(ctx context.Context, streamID StreamID) (StreamHead, error) {
	if streamID == "" {
		return StreamHead{}, fmt.Errorf("%w: stream id is required", ErrInvalidArgument)
	}
	return e.log.GetHead(ctx, streamID)
}

func (e *Engine) GetCursor(ctx context.Context, subscriberID SubscriberID, streamID StreamID) (SubscriberCursor, error) {
	if subscriberID == "" {
		return SubscriberCursor{}, fmt.Errorf("%w: subscriber id is required", ErrInvalidArgument)
	}
	if streamID == "" {
		return SubscriberCursor{}, fmt.Errorf("%w: stream id is required", ErrInvalidArgument)
	}
	return e.cursors.GetCursor(ctx, subscriberID, streamID)
}

func (e *Engine) CatchUp(ctx context.Context, req CatchUpRequest) (CatchUpResult, error) {
	if req.SubscriberID == "" {
		return CatchUpResult{}, fmt.Errorf("%w: subscriber id is required", ErrInvalidArgument)
	}
	if req.StreamID == "" {
		return CatchUpResult{}, fmt.Errorf("%w: stream id is required", ErrInvalidArgument)
	}

	limit := normalizeLimit(req.Limit)

	cursor, err := e.cursors.GetCursor(ctx, req.SubscriberID, req.StreamID)
	if err != nil {
		return CatchUpResult{}, err
	}
	head, err := e.log.GetHead(ctx, req.StreamID)
	if err != nil {
		return CatchUpResult{}, err
	}

	result := CatchUpResult{
		Status:           CatchUpStatusOK,
		Cursor:           cursor.Cursor,
		Head:             head,
		RetainedSeqStart: head.RetainedSeqStart,
	}
	if ReplayTooOld(cursor.Cursor.Seq, head.RetainedSeqStart) ||
		replayOverBudget(cursor.Cursor.Seq, head.Cursor.Seq, req.TotalLimit) {
		result.Status = CatchUpStatusTooLong
		return result, nil
	}

	batch, head, err := e.log.Read(ctx, req.StreamID, cursor.Cursor.Seq, limit)
	if err != nil {
		if errors.Is(err, ErrTooLong) {
			result.Status = CatchUpStatusTooLong
			return result, nil
		}
		return CatchUpResult{}, err
	}
	result.Batch = batch
	result.Head = head
	result.RetainedSeqStart = head.RetainedSeqStart
	return result, nil
}

func (e *Engine) Ack(ctx context.Context, req AckRequest) (AckResult, error) {
	if req.SubscriberID == "" {
		return AckResult{}, fmt.Errorf("%w: subscriber id is required", ErrInvalidArgument)
	}
	if req.StreamID == "" {
		return AckResult{}, fmt.Errorf("%w: stream id is required", ErrInvalidArgument)
	}

	head, err := e.log.GetHead(ctx, req.StreamID)
	if err != nil {
		return AckResult{}, err
	}
	if req.Seq > head.Cursor.Seq {
		return AckResult{}, fmt.Errorf("%w: ack seq %d is above stream head %d", ErrInvalidArgument, req.Seq, head.Cursor.Seq)
	}

	cursor, err := e.cursors.Ack(ctx, req)
	if err != nil {
		return AckResult{}, err
	}
	return AckResult{Cursor: cursor, Head: head}, nil
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return DefaultLimit
	}
	if limit > MaxLimit {
		return MaxLimit
	}
	return limit
}

// ReplayTooOld reports whether a committed cursor has fallen behind the stream's
// retention boundary and can no longer be served by contiguous replay. It is the
// single source of truth for the TOO_LONG boundary; storage adapters must use it
// instead of re-deriving the comparison so the engine pre-check and the adapter
// authoritative check never drift.
func ReplayTooOld(cursor Seq, retained Seq) bool {
	return retained > 0 && cursor < retained-1
}

func replayOverBudget(cursor Seq, head Seq, totalLimit int) bool {
	if totalLimit <= 0 || cursor >= head {
		return false
	}
	return head-cursor > Seq(totalLimit)
}
