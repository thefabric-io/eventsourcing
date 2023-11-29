package eventsourcing

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/ksuid"
	"time"
)

// Identifiable is an interface representing objects that can be identified by an Aggregate ID.
// In the context of this event sourcing system, it's used to ensure that the state of an event
// can provide an Aggregate ID when the event is applied to an aggregate.
type Identifiable interface {
	// AggregateID returns the ID of the aggregate that is associated with the identifiable object.
	AggregateID() string
}

// InceptionRecorder represents an interface for entities that are capable
// of recording the time at which they were brought into existence.
type InceptionRecorder[S AggregateState] interface {
	// RecordInception marks the time at which the entity was brought into existence.
	RecordInception(inceptionTime *time.Time, aggregate *Aggregate[S])
}

// ModificationRecorder represents an interface for entities that are capable
// of recording the time at which they were recently modified.
type ModificationRecorder[S AggregateState] interface {
	// RecordModification marks the time at which the entity was recently modified.
	RecordModification(modificationTime *time.Time, aggregate *Aggregate[S])
}

type Metadata map[string]any

func (m Metadata) Merge(m2 map[string]any) Metadata {
	res := make(Metadata, 0)

	for k, v := range m {
		res[k] = v
	}

	for k, v := range m2 {
		res[k] = v
	}

	return res
}

type EventState[S AggregateState] interface {
	Type() string
	Apply(aggregate *Aggregate[S])
}

// NewEvent creates a new event with the provided state and metadata.
func NewEvent[S AggregateState](event EventState[S], metadata map[string]any) *Event[S] {
	return &Event[S]{
		id:         fmt.Sprintf("evt_%s", ksuid.New().String()),
		occurredAt: time.Now(),
		_type:      event.Type(),
		aggregate:  nil,
		state:      event,
		metadata:   metadata,
	}
}

// InitEvent initializes an Event with provided id, occurredAt, state, aggregate, data, and metadata.
func InitEvent[S AggregateState](id string, occurredAt time.Time, state EventState[S], aggregate *Aggregate[S], data []byte, metadata []byte) (*Event[S], error) {
	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}

	var mtd map[string]any

	if err := json.Unmarshal(metadata, &mtd); err != nil {
		return nil, err
	}

	return &Event[S]{
		id:         id,
		occurredAt: occurredAt,
		_type:      state.Type(),
		aggregate:  aggregate,
		state:      state,
		metadata:   mtd,
	}, nil
}

// Event represents an event in event sourcing.
type Event[S AggregateState] struct {
	id         string
	occurredAt time.Time
	_type      string
	aggregate  *Aggregate[S]
	state      EventState[S]
	metadata   Metadata
}

// ID returns the ID of the event.
func (e *Event[S]) ID() string {
	return e.id
}

// Type returns the type of the event.
func (e *Event[S]) Type() string {
	return e._type
}

// Aggregate returns the aggregate associated with the event.
func (e *Event[S]) Aggregate() *Aggregate[S] {
	return e.aggregate
}

// Metadata returns the metadata associated with the event.
func (e *Event[S]) Metadata() Metadata {
	return e.metadata
}

// State returns the state of the event.
func (e *Event[S]) State() EventState[S] {
	return e.state
}

// OccurredAt returns the time when the event occurred.
func (e *Event[S]) OccurredAt() time.Time {
	return e.occurredAt
}

// Apply applies the Event to an Aggregate. In the context of event sourcing, state changes are recorded as a sequence of events.
// When an event is applied to an aggregate, it changes the state of the aggregate. Here's a detailed breakdown of the steps:
//
//   - It first increments the version of the Aggregate using the incrementVersion method. This helps in tracking the evolution of the aggregate.
//     Each event applied to the aggregate increases its version number.
//
//   - It then appends the Event itself to the changes slice of the Aggregate. This allows the aggregate to keep track of all events that have been applied to it.
//
//   - It checks whether the state of the Event implements the Identifiable interface. If it does, it sets the ID of the Aggregate to the ID
//     provided by the Event state's AggregateID method.
//
//   - It applies the state of the Event to the Aggregate using the Apply method of the EventState. This is where the actual state change occurs.
//
//   - Finally, it sets the Event's aggregate field to a clone of the updated Aggregate. This is crucial for keeping an accurate record of the
//     state of the Aggregate at the time when the event was applied.
//
// By enabling the event sourcing pattern in the provided Go codebase, this method allows the state of an Aggregate to evolve over time through the application of Events.
func (e *Event[S]) Apply(aggregate *Aggregate[S]) {
	aggregate.incrementVersion()

	aggregate.changes = append(aggregate.changes, e)

	if v, ok := e.State().(Identifiable); ok {
		aggregate.id = v.AggregateID()
	}

	if v, ok := e.State().(InceptionRecorder[S]); ok {
		v.RecordInception(&e.occurredAt, aggregate)
	}

	if v, ok := e.State().(ModificationRecorder[S]); ok {
		v.RecordModification(&e.occurredAt, aggregate)
	}

	aggregate.metadata = aggregate.metadata.Merge(e.metadata)

	e.State().Apply(aggregate)

	e.aggregate = aggregate.Clone()

	return
}

// MarshalJSON marshals the event into JSON format.
func (e *Event[S]) MarshalJSON() ([]byte, error) {
	type J struct {
		ID         string        `json:"id"`
		OccurredAt time.Time     `json:"occurred_at"`
		Type       string        `json:"type"`
		Aggregate  *Aggregate[S] `json:"aggregate"`
		State      EventState[S] `json:"state"`
		Metadata   Metadata      `json:"metadata,omitempty"`
	}

	res := J{
		ID:         e.id,
		OccurredAt: e.occurredAt,
		Type:       e._type,
		Aggregate:  e.aggregate,
		State:      e.State(),
		Metadata:   e.metadata,
	}

	return json.Marshal(&res)
}

func ParseEvent[S AggregateState](t string, eventsMap map[string]EventState[S]) (EventState[S], error) {
	var state S

	e, exists := eventsMap[t]
	if !exists {
		return nil, errors.New(fmt.Sprintf("cannot parse event type '%s' for '%s' aggregate", t, state.Type()))
	}

	return e.(EventState[S]), nil
}
