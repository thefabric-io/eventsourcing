# PostgreSQL Event Store

`pgeventstore` is the PostgreSQL implementation of the `eventsourcing.EventStore[S]` interface. It persists events to per‑aggregate tables and exposes methods to save, load and query events efficiently.

- Tables live under a configurable schema (default `eventsourcing`).
- Each aggregate type maps to a table named after `eventsourcing.StorageName(state)`.
- A global, monotonically increasing `offset` column supports cross‑aggregate, in‑order consumption.

## When to use

Use this package when you need a production‑ready event store backed by PostgreSQL with:
- JSONB storage for aggregate and event state
- Efficient paging by `offset`
- Batched inserts
- Simple bootstrapping and migrations

## Initialization

Provision the database schema and tables once at startup or via a migration step:

```go
config := pgeventstore.EventStorageConfig{
    PostgresURL: os.Getenv("EVENT_STORE_PG_URL"),
    Schema:      envOr("EVENT_STORE_SCHEMA", "eventsourcing"),
    Aggregates:  "user,order", // comma‑separated list of table names
}
if err := pgeventstore.Init(config); err != nil {
    log.Fatal(err)
}
```

Environment variables are supported if you call `Init()` without arguments:
- `EVENT_STORE_PG_URL`
- `EVENT_STORE_SCHEMA` (defaults to `eventsourcing` when empty)
- `EVENT_STORE_AGGREGATES` (comma‑separated list)

`Init` will:
- Ensure the schema exists
- Create one table per aggregate
- Create helper indexes
- Create/upgrade the `offset` BIGSERIAL column if missing
- Create the `events_consumers` table used by `eventconsumer/pgeventconsumer`

Note: The `eventconsumer/pgeventconsumer` package currently queries the `events_consumers` table under the `eventstore` schema. If you use that package, set `EVENT_STORE_SCHEMA=eventstore` (or pass `Schema: "eventstore"`) so both event tables and `events_consumers` are created where the consumer expects them.

## API

Create a typed store and use it with your aggregate state:

```go
store := pgeventstore.Storage[*MyAggregateState]()

// Load last or specific version
agg, err := store.Load(ctx, tx, aggregateID, eventsourcing.LastVersion)

// Fetch cross‑aggregate events by offset and type
events, err := store.Events(ctx, tx, eventsourcing.EventsParams{
    AfterOffset: cursor,
    TypesOnly:   []string{"user_created", "user_updated"},
    Limit:       100,
})

// Persist events
if err := store.Save(ctx, tx, aggregate1, aggregate2); err != nil { /* ... */ }
```

## Mapping event types

To parse events, your aggregate state must implement:

```go
// AggregateStateEventMapper declares all event types for the aggregate
// and provides a prototype for each event so the store can unmarshal states.
//
// Example:
// func (UserState) EventsMap() map[string]eventsourcing.EventState[*UserState] {
//     return map[string]eventsourcing.EventState[*UserState]{
//         "user_created":  &UserCreated{},
//         "user_renamed":  &UserRenamed{},
//     }
// }
```

## Storage name customization

By default, tables are named after the aggregate state's `Type()`. Implement `eventsourcing.StorageNamer` to customize:

```go
func (UserState) StorageName() string { return "users" }
```

## Performance notes

- Inserts are chunked in batches up to 500 rows.
- `Events` defaults to a `Limit` of 1000 when empty.
- Use indexes created by `Init` to keep queries fast under growth.

## Breaking changes & migration

- Introduced global `offset` and `Events` API for cross‑aggregate streaming. Consumers should switch to retrieving by offset rather than per‑aggregate version.
- `AggregateState` must provide an `EventsMap()` via `AggregateStateEventMapper[S]` when using `Events(...)` so the store can parse event states.
- Optional `StorageNamer` allows table names to differ from `Type()`; existing code remains compatible if you do nothing.

## Troubleshooting

- Ensure you pass the correct generic state type `S` consistently across store, subscriber and handlers.
- If parsing fails with "cannot parse event type", verify your `EventsMap()` contains the type string stored in the DB.
- For high‑throughput consumers, tune Postgres (work_mem, shared_buffers, WAL settings) and consider larger `Limit` values.
