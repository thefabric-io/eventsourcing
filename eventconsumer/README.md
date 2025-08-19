# Event Consumers

The `eventconsumer` package provides a robust, production‑ready framework for consuming events from an event‑sourced stream with offset tracking and at‑least‑once delivery semantics.

It ships with two subscriber implementations so you can pick the execution model that fits your workload:

- **AsyncSequentialSubscriber**: sequential processing, no open DB transaction during handler execution (best for IO‑heavy or long‑running handlers).
- **FIFOSubscriber**: sequential processing inside a DB transaction (best for transactional projections that must commit together with offset updates).

Both models keep event order and manage offsets via a pluggable `ConsumerStore` interface. A PostgreSQL-backed implementation lives in `eventconsumer/pgeventconsumer`.

## Key concepts

- **Subscriber[S]**: runs a loop to fetch, dispatch and acknowledge events for aggregate state `S`.
- **Handler[S]**: async (non-transactional) event handler for `AsyncSequentialSubscriber`.
- **TxHandler[S]**: transactional handler for `FIFOSubscriber`.
- **ConsumerStore**: persistence for `offset_acked`, `offset_consumed` and `last_occurred_at`.

Offsets are managed with two cursors:
- **offset_consumed**: highest offset currently reserved/being processed.
- **offset_acked**: highest contiguous offset fully processed and persisted.

This separation enables batching, efficient restarts, and deterministic recovery without skipping gaps.

## Public interfaces

```go
// Subscriber starts and reports status; concrete subscribers expose their own
// RegisterHandler variant matching the handler type they support (Handler or TxHandler).
type Subscriber[S eventsourcing.AggregateState] interface {
    Start(ctx context.Context) error
    Status() ConsumerStatus
}

// Async (no transaction)
type Handler[S eventsourcing.AggregateState] interface {
    HandleEvent(ctx context.Context, ev *eventsourcing.Event[S]) error
}

// FIFO (transaction available)
type TxHandler[S eventsourcing.AggregateState] interface {
    HandleEvent(ctx context.Context, tx transactional.Transaction, ev *eventsourcing.Event[S]) error
}

// Adapter to reuse a TxHandler with the async subscriber
func FromTxHandler[S eventsourcing.AggregateState](h TxHandler[S]) Handler[S]

// ConsumerStore persistence
type ConsumerStore interface {
    Save(ctx context.Context, tx transactional.Transaction, name string, offsetAcked, offsetConsumed int) error
    Load(ctx context.Context, tx transactional.Transaction, name string) (Consumer, error)
}
```

## AsyncSequentialSubscriber (non‑transactional)

Purpose: keep DB transactions short while processing events strictly in order. Ideal for IO, HTTP calls, email, queues, or any handler that should not run under an open DB transaction.

Construction (parameter object + options):

```go
params := eventconsumer.NewAsyncSequentialParams[
    *MyAggregateState,
](tx, "my-consumer-group", consumerStore, eventStore)

sub := eventconsumer.NewAsyncSequentialSubscriber[
    *MyAggregateState,
](ctx, params,
    eventconsumer.WithBatchSize[*MyAggregateState](100),
    eventconsumer.WithWaitTimes[*MyAggregateState](1*time.Second, 100*time.Millisecond),
    eventconsumer.WithAckBatch[*MyAggregateState](10, 500*time.Millisecond),
    eventconsumer.WithHandlerTimeout[*MyAggregateState](2*time.Second),
    eventconsumer.WithRetryPolicy[*MyAggregateState](100*time.Millisecond, 5*time.Second, 2.0),
)

sub.RegisterHandler("my-event", &MyAsyncHandler{})
```

Behavior highlights:
- Fetches events in short transactions, then processes them outside any transaction.
- Preserves in‑order processing per consumer group.
- Batches acknowledgments with contiguous‑ack logic and gap bootstrap (handles non‑1 or 0‑based streams and retention gaps).
- Bounded queues, non‑blocking enqueue with jittered backoff, graceful shutdown that drains and flushes acks.

## FIFOSubscriber (transactional)

Purpose: keep the event handler and the offset update in the same DB transaction. Ideal for strictly transactional projections.

Construction:

```go
fifo := eventconsumer.NewFIFOSubscriber[*MyAggregateState](
    ctx,
    tx,
    eventconsumer.NewFIFOSubscriberParams[*MyAggregateState]{
        Name:             "my-consumer-group",
        ConsumerStore:    consumerStore,
        EventStore:       eventStore,
        BatchSize:        100,
        WaitTime:         1 * time.Second,
        WaitTimeIfEvents: 100 * time.Millisecond,
    },
)
fifo.RegisterHandlerTx("my-event", &MyTxHandler{})
```

Behavior highlights:
- Opens a transaction per polling cycle, fetches, processes, and updates offsets atomically.
- Retries on next loop if the handler fails (transaction rolled back).

## Wiring and running multiple subscribers

```go
manager := eventconsumer.NewManager[*MyAggregateState]()
manager.AddProcessors(sub, fifo)
manager.Run(ctx)
```

## Storage integration (PostgreSQL)

Use `eventconsumer/pgeventconsumer` for a ready‑to‑use PostgreSQL `ConsumerStore` and a convenience constructor for the async subscriber. Simply import the package to register a default store provider, or call `pgeventconsumer.PostgresConsumerStore()` explicitly.

```go
// Convenience constructor (uses default Postgres store via import side-effect)
sub := pgeventconsumer.NewAsyncSequentialSubscriber[
    *MyAggregateState,
](ctx, tx, "my-consumer-group", eventStore,
    eventconsumer.WithBatchSize[*MyAggregateState](100),
)
```

## Migration and breaking changes

- **Handler split**: A single handler that previously received a transaction has been split:
  - Use `Handler[S]` for async (non‑transactional) processing.
  - Use `TxHandler[S]` for transactional processing.
  - Migrate by implementing the appropriate interface or wrap an existing `TxHandler` with `FromTxHandler` for async use.

- **AsyncSequentialSubscriber constructor**: moved to a parameter object + functional options:
  - Before: `NewAsyncSequentialSubscriber(ctx, tx, name, consumerStore, eventStore, batch, wait, waitIf, ackBatchSize, ackTimeout)`
  - Now: `NewAsyncSequentialSubscriber(ctx, NewAsyncSequentialParams(tx, name, consumerStore, eventStore), WithBatchSize(...), WithWaitTimes(...), WithAckBatch(...), ...)`
  - Alternatively use `pgeventconsumer.NewAsyncSequentialSubscriber` which injects the Postgres store for you.

## Best practices

- Use AsyncSequential for long‑running work; use FIFO only when you truly need transactional coupling.
- Start small batch sizes, measure, then tune `WithBatchSize`, `WithAckBatch`, and `WithWaitTimes`.
- Register handlers for every event type you intend to process. AsyncSequential will auto‑ack unhandled types so the cursor can advance.
- Use `context.Context` to drive shutdowns; for AsyncSequential call `Close()` if you created the subscriber without the manager.

---

For Postgres storage wiring and helpers, see `eventconsumer/pgeventconsumer`. For the event store implementation, see `pgeventstore`.


