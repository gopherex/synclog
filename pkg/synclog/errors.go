package synclog

import "errors"

var (
	// ErrInvalidArgument is returned when a request cannot be interpreted.
	ErrInvalidArgument = errors.New("synclog: invalid argument")
	// ErrNotFound is returned when an exact object lookup has no result.
	ErrNotFound = errors.New("synclog: not found")
	// ErrTooLong is returned when replay from a cursor is no longer possible or
	// exceeds a caller-supplied replay budget.
	ErrTooLong = errors.New("synclog: replay too long")
)
