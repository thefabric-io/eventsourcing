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

## Usage

To use this library, you would define your own aggregate states and event states that satisfy the `AggregateState` and `EventState` interfaces, respectively. Then, you can create events and apply them to aggregates, and store the aggregates in an event store.

**WIP, TODO: Add usage examples**

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

## Contributing

Feel free to submit a pull request if you find any bugs or you want to make improvements to the library. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License.

---
This Go library provides a solid foundation for working with Event Sourcing in Go. It is designed with flexibility in mind to allow it to be adapted to various application needs. We hope this library proves to be a valuable tool in your software development toolkit.
