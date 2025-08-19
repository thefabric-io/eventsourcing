# PostgreSQL Consumer Store for Event Consumers

The `eventconsumer/pgeventconsumer` package provides a PostgreSQL‑backed implementation of the `eventconsumer.ConsumerStore` interface. It persists consumer progress (`offset_acked`, `offset_consumed`) in the `events_consumers` table and offers a convenience constructor for the async subscriber that auto‑wires the store.

## Features

- Durable offset tracking with an idempotent UPSERT on `(consumer_group, aggregate_type)`.
- Safe bootstrap on first run (row is created on demand).
- Convenience constructor for `AsyncSequentialSubscriber` that injects the Postgres store by default.
- Import side‑effect to set a default `ConsumerStore` so the core async subscriber works out of the box when this package is imported.

## Schema

This package expects a table named `events_consumers` in your event store schema (default `eventsourcing`). The table is provisioned by `pgeventstore.Init(...)`:

```sql
create table if not exists eventsourcing.events_consumers(
  consumer_group    varchar(255) not null,
  aggregate_type    varchar,
  offset_acked      bigint,
  offset_consumed   bigint not null,
  last_occurred_at  timestamptz,
  primary key (consumer_group, aggregate_type)
);
```

## Usage

### Option A: Explicit store

```go
store := pgeventconsumer.PostgresConsumerStore()
params := eventconsumer.NewAsyncSequentialParams[*MyAggregateState](tx, "my-consumer-group", store, eventStore)
sub := eventconsumer.NewAsyncSequentialSubscriber[*MyAggregateState](ctx, params)
```

### Option B: Convenience constructor (recommended)

```go
// Import registers a default store provider via init(), so you can omit the explicit store
import (
    _ "github.com/thefabric-io/eventsourcing/eventconsumer/pgeventconsumer"
)

sub := pgeventconsumer.NewAsyncSequentialSubscriber[*MyAggregateState](ctx, tx, "my-consumer-group", eventStore)
```

Both variants require that your database schema has been bootstrapped by `pgeventstore.Init(...)`.

## Notes and caveats

- `aggregate_type` is stored but not currently filtered by subscribers; it is reserved for future features and partitioning.
- `offset_consumed` may be ahead of `offset_acked` while a batch is in flight; this is expected. Recovery resumes after the last acked offset.

## Migration hints (breaking changes)

- If you previously passed a `ConsumerStore` directly to the async subscriber constructor, consider switching to the new parameter‑object constructor or the convenience wrapper here.
- Importing this package now sets `eventconsumer.DefaultConsumerStoreProvider`. If you provide your own store, pass it explicitly and it will take precedence.


