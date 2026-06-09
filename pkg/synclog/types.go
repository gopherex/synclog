package synclog

import "time"

const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

type StreamID string
type SubscriberID string
type Seq uint64

type Event struct {
	StreamID        StreamID
	Seq             Seq
	Payload         []byte
	PayloadType     string
	PayloadVersion  uint32
	IdempotencyKey  string
	CreatedAtUnixMS int64
}

type Cursor struct {
	StreamID StreamID
	Seq      Seq
}

type SubscriberCursor struct {
	SubscriberID    SubscriberID
	Cursor          Cursor
	Metadata        string
	UpdatedAtUnixMS int64
}

type StreamHead struct {
	Cursor
	// RetainedSeqStart is the first sequence number still available for replay.
	// A value of zero means the stream has no retained events.
	RetainedSeqStart Seq
}

type EventBatch struct {
	Events   []Event
	SeqStart Seq
	Seq      Seq
	Final    bool
}

type AppendRequest struct {
	StreamID        StreamID
	Payload         []byte
	PayloadType     string
	PayloadVersion  uint32
	IdempotencyKey  string
	CreatedAtUnixMS int64
}

type AppendResult struct {
	Event        Event
	Deduplicated bool
}

type CatchUpStatus int

const (
	CatchUpStatusNone CatchUpStatus = iota
	CatchUpStatusOK
	CatchUpStatusTooLong
)

type CatchUpRequest struct {
	SubscriberID SubscriberID
	StreamID     StreamID
	Limit        int
	TotalLimit   int
}

type CatchUpResult struct {
	Status           CatchUpStatus
	Batch            EventBatch
	Cursor           Cursor
	Head             StreamHead
	RetainedSeqStart Seq
}

type AckRequest struct {
	SubscriberID SubscriberID
	StreamID     StreamID
	Seq          Seq
	Metadata     string
}

type AckResult struct {
	Cursor SubscriberCursor
	Head   StreamHead
}

func UnixMS(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
