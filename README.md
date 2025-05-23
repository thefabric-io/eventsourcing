# Go Event Sourcing Library

[![Go Report Card](https://goreportcard.com/badge/github.com/thefabric-io/eventsourcing)](https://goreportcard.com/report/github.com/thefabric-io/eventsourcing)


This is a robust and flexible Go library designed for implementing the Event Sourcing pattern in your applications, leveraging the power of Go's generics. Event Sourcing is a design pattern that enables you to capture changes to an application state as a sequence of events, rather than just storing the current state.

The use of generics in this library provides several advantages:

1. **Type Safety**: Generics allow for compile-time type checking, reducing the likelihood of runtime errors and increasing overall code safety.
2. **Code Reusability**: With generics, you can write functions or types that are abstracted over types, meaning they can be reused with different types.
3. **Improved Performance**: Generics can lead to performance improvements as they eliminate the need for type assertions and reflection.
4. **Clearer and Cleaner Code**: Generics result in code that's easier to read, understand, and maintain.

The library includes key components such as Aggregates, Events, and an Event Store, all fundamental to the Event Sourcing architecture. By facilitating an efficient method of handling data changes, this library serves as a valuable tool for maintaining and auditing the complete history of data changes over time in your applications.

## Features

This Go Event Sourcing Library offers a broad range of features to make implementing Event Sourcing in your Go applications as straightforward and efficient as possible. Here's a rundown of the key features this library provides:

1. **Generics-Based Architecture**: The library leverages Go's generics capabilities to provide type-safe and reusable components, resulting in clearer and cleaner code, improved performance, and reduced likelihood of runtime errors.

2. **Aggregate Management**: It provides robust support for managing aggregates, entities with unique IDs whose state evolves over time through a sequence of events. Aggregates also have invariants that represent business rules to maintain consistency.

3. **Event Handling**: The library offers comprehensive tools for creating and handling events, which are fundamental units of state change in the Event Sourcing pattern. It includes functionality to apply events to aggregates, track event metadata, and even marshal events to JSON.

4. **Event Store Interface**: It provides a standard interface for an Event Store, the storage system for events, enabling the saving, loading, and retrieving of an aggregate's history. This interface can be implemented with various storage systems according to your application's needs.

5. **Version Management**: It includes functionality for managing the version of an aggregate, which is incremented each time an event is applied. This feature aids in tracking the evolution of the aggregate over time.

These features come together to provide a solid foundation for implementing the Event Sourcing pattern in your Go applications, facilitating efficient data change handling and thorough auditing of changes over time.

Certainly! Here's a completed section that references the to-do list example as a practical illustration of how to use the event sourcing library:

## Usage

To use this library, you would define your own aggregate states and event states that satisfy the `AggregateState` and `EventState` interfaces, respectively. Then, you can create events and apply them to aggregates, and store the aggregates in an event store.

### Usage Examples

For a hands-on illustration of how to apply this library, check out our to-do list example. This repository demonstrates a simple yet effective implementation of event sourcing in a practical application: managing a to-do list. You'll see how aggregate and event states are defined and utilized within a Go application, providing a clear blueprint for incorporating event sourcing into your projects.

- **Example Repository**: [Event Sourcing To-Do Example](https://github.com/thefabric-io/eventsourcing.example)

In this example, you will learn how to:

- Define aggregate states for your to-do items and event states for actions such as creating a to-do or adding a task.
- Create and apply events to these aggregates, effectively demonstrating the event sourcing process.
- Utilize the Command Query Responsibility Segregation (CQRS) pattern to separate the read and write operations of your application, enhancing its architecture and scalability.

This example serves as a practical guide to understanding and implementing event sourcing with our library. By exploring the repository, you can gain insights into structuring your application for event sourcing and managing state through events in a real-world scenario.

## Main Components

### Aggregate

`Aggregate` represents an entity in your system that is identified by a unique ID. It encapsulates state that evolves over time through a sequence of events.

```go
type Aggregate[S AggregateState] struct {...}
```

### Event

`Event` represents something that has occurred within your system. In the context of event sourcing, events are the source of truth and they determine the current state of an aggregate. Each event is associated with an aggregate and contains data that describes the state change.

```go
type Event[S AggregateState] struct {...}
```

### Event Store

`EventStore` is the storage interface for events. It provides methods to save, load, and retrieve the history of an aggregate.

```go
type EventStore[S AggregateState] interface {...}
```

## Key Interfaces

### AggregateState

`AggregateState` interface represents the state of an aggregate. It defines the `Type` method to return the type of the aggregate, and the `Zero` method to return a zero-valued instance of the aggregate state.

```go
type AggregateState interface {...}
```

### EventState

`EventState` is an interface representing an event state that can be applied to an aggregate. It has `Type` method to return the type of the event and `Apply` method to apply the event to an aggregate.

```go
type EventState[S AggregateState] interface {...}
```

### Identifiable

`Identifiable` is an interface representing objects that can be identified by an Aggregate ID. It's used to ensure that the state of an event can provide an Aggregate ID when the event is applied to an aggregate.

```go
type Identifiable interface {...}
```

### InceptionRecorder

`InceptionRecorder` is an interface representing objects that can record the inception date of an aggregate. It's used to ensure that the state of an event can record the inception date of an aggregate when the event is applied to an aggregate.

```go
type InceptionRecorder interface {...}
```

### ModificationRecorder

`ModificationRecorder` is an interface representing objects that can record the modification date of an aggregate. It's used to ensure that the state of an event can record the modification date of an aggregate when the event is applied to an aggregate.

```go
type ModificationRecorder interface {...}
```

### StorageNamer Interface

The `v0.2.0` release introduces a significant enhancement to the Go Event Sourcing Library: the `StorageNamer` interface. This interface provides a customizable way for aggregates to define their corresponding storage entities, such as database tables or collections. This feature is especially beneficial as your project scales up, offering a more adaptable approach to storage naming conventions.

#### Definition:

```go
type StorageNamer interface {
    StorageName() string
}
```

#### Usage:

- Implement the `StorageNamer` interface in your aggregate state if you require a custom storage name different from the default aggregate state type name.
- If an aggregate state does not implement `StorageNamer`, the library defaults to using the aggregate state's type as the storage identifier.

#### Example:

Suppose you have an aggregate state `UserState`. By default, the storage name would be `UserState`. If you need a different storage name, say `user_table`, implement the `StorageNamer` interface as shown below:

```go
type UserState struct {...}

func (u *UserState) StorageName() string {
    return "user_table"
}

func (u *UserState) Type() string {
    return "user"
}
```

This implementation allows `UserState` to explicitly specify `user_table` as its storage name.

### Benefits of Using StorageNamer

- **Explicit Naming**: Offers a clear and explicit way to define storage names, enhancing readability and maintainability of your code.
- **Flexibility**: Adapts to various storage backends and user requirements, making the library more versatile.
- **Backward Compatibility**: Ensures existing codebases remain functional without modifications.

### Implementation Considerations

- This interface is optional; you can choose to implement it based on your project's requirements.
- Ensure consistency in the naming conventions used across your project for clarity.

## PostgreSQL Event Store Implementation

The Go Event Sourcing Library includes a PostgreSQL implementation of the Event Store interface. This provides the ability to persist and query your events in a PostgreSQL database, giving you the ability to leverage robust and scalable SQL database capabilities for your Event Sourcing needs.

The PostgreSQL Event Store implementation is located in the `pgeventstore` package, and includes the following key functionalities:

1. **Database Setup and Initialization**: The library must be initialized with the necessary environment variables or config for your database connection and schema name, and establishes a connection to the PostgreSQL server. If necessary, it will also create the schema and corresponding tables for the specified aggregates in your database.

2. **Event Marshaling**: The library provides a mechanism to marshal your events into a format that can be stored in PostgreSQL. It takes care of null checks and type conversions, ensuring your data is safe and reliable.

3. **Event Store Interface Implementation**: This includes the following operations:

    - `Load`: Retrieves the latest aggregate state based on the aggregate ID and version number.
    - `History`: Returns the history of changes to an aggregate, based on the aggregate ID, starting from a specific version.
    - `Save`: Persists the changes to an aggregate into the PostgreSQL database.

The implementation also includes functionalities like creating the necessary tables and indexes in your PostgreSQL database and handling batch inserts to optimize the storage of events.

The PostgreSQL Event Store implementation is designed to be highly scalable, capable of handling large volumes of events and providing fast query performance. It leverages PostgreSQL's capabilities for handling JSON data types, making it an ideal choice for Event Sourcing applications.

For the detailed code of this implementation, please refer to the `pgeventstore` package in the source code. Be sure to initialize library properly for your PostgreSQL server's URL and other necessary details before running your application.

Remember to carefully handle your database connections and transactions to ensure data consistency and reliability. The library supports transaction management, allowing you to handle operations atomically and safely.

**Note**: Always ensure that your PostgreSQL server is properly configured and optimized for your workload. Performance can vary based on server specifications, data volume, query complexity, and other factors.

### Configuration

The PostgreSQL Event Store implementation requires certain config variables to be set for its proper functioning. These are:

- `EVENT_STORE_PG_URL`: This should be the connection string for your PostgreSQL database. This must be a valid connection string that includes the username, password, host, port, and database name. For example, `postgresql://user:pass@localhost:5432/mydatabase`.

- `EVENT_STORE_SCHEMA`: This variable represents the name of the schema under which your event tables will be created. If not provided, the implementation will default to using `eventsourcing` as the schema name.

- `EVENT_STORE_AGGREGATES`: A comma-separated list of all aggregate tables to be created in your PostgreSQL database. These tables will hold your events.

For example, if you have a `user` and `order` aggregates, you can set `EVENT_STORE_AGGREGATES=user,order`.

You can also initialize using pgeventstore.EventStorageConfig struct:
```go
config := pgeventstore.EventStorageConfig{
    PostgresURL: "postgres://user:pass@localhost:5432?sslmode=disable",
    Schema:      "eventsourcing",
    Aggregates:  "user,order",
}

if err := pgeventstore.Init(config); err != nil {
    return err
}
```

The library uses the [godotenv](https://github.com/joho/godotenv) package for loading environment variables from a `.env` file. You can also set these environment variables manually in your deployment environment.

For security reasons, make sure that your database connection string (`EVENT_STORE_PG_URL`) is kept secure and not exposed in your code or version control system.

Remember to restart your application after changing your environment variables to ensure the changes take effect.

## Event Consumer Mechanism

### Overview

The `eventconsumer` package provides a robust and easy-to-use mechanism for consuming events from an event-sourced system. It is designed to help you build projections, background processors, and other event-driven components that need to reliably process events with offset tracking and at-least-once delivery semantics.

The `eventconsumer/pgeventconsumer` package offers a PostgreSQL-backed implementation for storing consumer offsets, making it easy to persist and recover consumer state across restarts.

### Why Use This Mechanism?

- **Reliable Offset Tracking:** Ensures that each event is processed exactly once per consumer group, even in the face of failures.
- **Easy Integration:** Simple interfaces for registering handlers and starting consumers.
- **Supports Projections:** Ideal for building read models and projections that need to process all events in order.
- **Pluggable Storage:** Use the provided PostgreSQL implementation or create your own by implementing the `ConsumerStore` interface.

### How It Works

- **ConsumerStore:** Abstracts the storage and retrieval of consumer offsets. The `pgeventconsumer` package provides a ready-to-use implementation for PostgreSQL.
- **Subscriber:** Represents a consumer that processes events. You register handlers for event types, and the subscriber manages fetching and dispatching events to the appropriate handler.
- **Manager:** Allows you to manage and run multiple subscribers concurrently.

#### Main Interfaces

```go
// ConsumerStore is an interface for storing and retrieving consumer offsets.
type ConsumerStore interface {
    Save(ctx context.Context, transaction transactional.Transaction, name string, offsetAcked, offsetConsumed int) error
    Load(ctx context.Context, transaction transactional.Transaction, name string) (Consumer, error)
}

// Subscriber is responsible for receiving and processing messages.
type Subscriber[S eventsourcing.AggregateState] interface {
    Start(ctx context.Context) error
    RegisterHandler(messageType string, handler Handler[S])
    UnregisterHandler(messageType string) error
    Status() ConsumerStatus
}
```

#### PostgreSQL Implementation

The `pgeventconsumer` package provides a `PostgresConsumerStore` that persists consumer offsets in a PostgreSQL table. This allows consumers to resume processing from the last acknowledged event after a restart.

### Usage Example

```go
import (
    "context"

    "github.com/thefabric-io/eventsourcing"
    "github.com/thefabric-io/eventsourcing/eventconsumer"
    "github.com/thefabric-io/eventsourcing/eventconsumer/pgeventconsumer"
    "github.com/thefabric-io/eventsourcing/pgeventstore"
    "github.com/thefabric-io/transactional"
    "github.com/thefabric-io/transactional/pgtransactional"
    

    )

// Define your aggregate state and event handler
type MyAggregateState struct{}

// Implement eventconsumer.Handler for your event type
type MyHandler struct{}
func (h *MyHandler) HandleEvent(ctx context.Context, tx transactional.Transaction, ev *eventsourcing.Event[*MyAggregateState]) error {
    // Process the event
    return nil
}

func main() {
    ctx := context.Background()
	
    tx, err := pgtransactional.InitSQLXTransactionalConnection(ctx, os.Getenv("TRANSACTIONAL_DATABASE_URL"))
    if err != nil {
        log.Fatalln(err)
    }
	
    eventStore := pgeventstore.Storage[*MyAggregateState]()
    consumerStore := pgeventconsumer.PostgresConsumerStore()

    params := eventconsumer.NewFIFOSubscriberParams[MyAggregateState]{
        Name:             "my-consumer-group",
        ConsumerStore:    consumerStore,
        EventStore:       eventStore,
        BatchSize:        100,
        WaitTime:         1 * time.Second,
        WaitTimeIfEvents: 100 * time.Millisecond,
    }

    subscriber := eventconsumer.NewFIFOConsumer(ctx, tx, params)
    subscriber.RegisterHandler("MyEventType", &MyHandler{})

    manager := eventconsumer.NewManager[MyAggregateState]()
    manager.AddProcessors(subscriber)
    manager.Run(ctx)
}
```

### Technical Details

- The FIFO consumer fetches events in batches, processes them using registered handlers, and updates the consumer offset in the store.
- If no events are available, it waits for a configurable duration before polling again.
- The PostgreSQL consumer store uses an upsert strategy to persist offsets, ensuring idempotency and reliability.


## Contributing

Feel free to submit a pull request if you find any bugs or you want to make improvements to the library. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License.

---
This Go library provides a solid foundation for working with Event Sourcing in Go. It is designed with flexibility in mind to allow it to be adapted to various application needs. We hope this library proves to be a valuable tool in your software development toolkit.
