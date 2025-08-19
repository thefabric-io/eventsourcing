package eventconsumer

import (
	"context"

	"github.com/thefabric-io/eventsourcing"
	"github.com/thefabric-io/transactional"
)

// Subscriber is responsible for receiving and processing messages. It provides methods
// for registering and unregistering handlers, starting the subscription process, and
// retrieving the current status of the subscription.
//
// Note: concrete subscribers expose their own RegisterHandler API matching
// the handler type they support (Handler or TxHandler).
type Subscriber[S eventsourcing.AggregateState] interface {
	// Start starts the subscription process. It takes a context for cancellation and timeouts.
	Start(ctx context.Context) error

	// Status returns the current status of the subscription.
	Status() ConsumerStatus
}

// Handler processes an event outside any DB transaction (used by AsyncSequentialSubscriber).
type Handler[S eventsourcing.AggregateState] interface {
	HandleEvent(ctx context.Context, ev *eventsourcing.Event[S]) error
}

// TxHandler processes an event inside an open transactional transaction (used by FIFO).
type TxHandler[S eventsourcing.AggregateState] interface {
	HandleEvent(ctx context.Context, tx transactional.Transaction, ev *eventsourcing.Event[S]) error
}

// FromTxHandler adapts a TxHandler to a non-transactional Handler by ignoring the tx.
// Useful to reuse the same handler with AsyncSequentialSubscriber.
func FromTxHandler[S eventsourcing.AggregateState](h TxHandler[S]) Handler[S] {
	return asyncHandlerAdapter[S]{inner: h}
}

type asyncHandlerAdapter[S eventsourcing.AggregateState] struct {
	inner TxHandler[S]
}

func (a asyncHandlerAdapter[S]) HandleEvent(ctx context.Context, ev *eventsourcing.Event[S]) error {
	return a.inner.HandleEvent(ctx, nil, ev)
}

type ConsumerStatus struct {
	ProcessedEventCount int
	// Other relevant status fields
}
