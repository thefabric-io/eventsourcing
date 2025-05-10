package eventconsumer

import (
	"context"
	"time"

	"github.com/thefabric-io/transactional"
)

// ConsumerStore is an interface for storing and retrieving consumer offsets.
type ConsumerStore interface {
	Save(ctx context.Context, transaction transactional.Transaction, name string, offsetAcked, offsetConsumed int) error
	Load(ctx context.Context, transaction transactional.Transaction, name string) (Consumer, error)
}

// Consumer represents a consumer of events. It provides information about the consumer,
// such as its group, aggregate type, and offsets.
type Consumer interface {
	Group() string
	AggregateType() string
	OffsetAcked() int
	OffsetConsumed() int
	LastOccurred() *time.Time
}
