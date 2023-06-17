package eventsourcing

import (
	"context"
)

type Transaction interface {
	Commit() error
	Rollback() error
}

// EventStore represents a storage for events. It provides methods to save, load, and retrieve the history of an aggregate.
type EventStore[S AggregateState] interface {
	Save(ctx context.Context, tx Transaction, aggregate *Aggregate[S]) (err error)
	Load(ctx context.Context, tx Transaction, aggregateID string, version AggregateVersion) (*Aggregate[S], error)
	History(ctx context.Context, tx Transaction, aggregateID string, fromVersion int, limit int) ([]*Event[S], error)
}

type LoadParams[S AggregateState] struct {
	AggregateID      string
	Aggregate        S
	AggregateVersion AggregateVersion
}

type HistoryParams struct {
	AggregateID      string
	AggregateVersion AggregateVersion
}

type AggregateVersion int

const LastVersion = 0
