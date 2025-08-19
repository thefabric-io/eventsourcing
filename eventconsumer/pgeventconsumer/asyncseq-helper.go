package pgeventconsumer

import (
	"context"

	"github.com/thefabric-io/eventsourcing"
	"github.com/thefabric-io/eventsourcing/eventconsumer"
	"github.com/thefabric-io/transactional"
)

func init() {
	eventconsumer.DefaultConsumerStoreProvider = func() eventconsumer.ConsumerStore { return PostgresConsumerStore() }
}

// NewAsyncSequentialSubscriber is a Postgres-convenience wrapper that provides
// the Postgres-backed ConsumerStore automatically. This avoids passing
// PostgresConsumerStore() at each call site while keeping the core package
// decoupled from storage implementations.
func NewAsyncSequentialSubscriber[S eventsourcing.AggregateState](
	ctx context.Context,
	tx transactional.Transactional,
	name string,
	eventStore eventsourcing.EventStore[S],
	opts ...eventconsumer.AsyncSequentialSubscriberOption[S],
) *eventconsumer.AsyncSequentialSubscriber[S] {
	// Register default provider on first use to let core resolve it implicitly
	if eventconsumer.DefaultConsumerStoreProvider == nil {
		eventconsumer.DefaultConsumerStoreProvider = func() eventconsumer.ConsumerStore { return PostgresConsumerStore() }
	}
	params := eventconsumer.NewAsyncSequentialParams[S](tx, name, nil, eventStore)
	return eventconsumer.NewAsyncSequentialSubscriber[S](ctx, params, opts...)
}
