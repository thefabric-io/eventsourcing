package eventsourcing

import "errors"

var (
	// ErrAggregateNotFound is returned when an aggregate is not found.
	ErrAggregateNotFound = errors.New("aggregate not found")
)
