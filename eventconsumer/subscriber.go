package eventconsumer

import (
	"context"

	"github.com/thefabric-io/eventsourcing"
	"github.com/thefabric-io/transactional"
)

// Subscriber is responsible for receiving and processing messages. It provides methods
// for registering and unregistering handlers, starting the subscription process, and
// retrieving the current status of the subscription.
type Subscriber[S eventsourcing.AggregateState] interface {
	// Start starts the subscription process. It takes a context for cancellation and timeouts.
	Start(ctx context.Context) error

	// RegisterHandler associates a message type with a handler.
	RegisterHandler(messageType string, handler Handler[S])

	// UnregisterHandler removes the association between a message type and a handler.
	UnregisterHandler(messageType string) error

	// Status returns the current status of the subscription.
	Status() ConsumerStatus
}

// Handler is responsible for processing messages. It provides a single method for processing messages.
type Handler[S eventsourcing.AggregateState] interface {
	// HandleEvent processes a message. It takes a context for cancellation and timeouts, and the message to process.
	HandleEvent(ctx context.Context, tx transactional.Transaction, ev *eventsourcing.Event[S]) error
}

type ConsumerStatus struct {
	ProcessedEventCount int
	// Other relevant status fields
}
