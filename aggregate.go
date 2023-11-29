package eventsourcing

import (
	"encoding/json"
)

// AggregateState is an interface defining methods that should be implemented by any state that is part of an aggregate.
type AggregateState interface {
	Type() string         // Returns the type of the aggregate state.
	Zero() AggregateState // Returns a zero value of the aggregate state.
}

type AggregateStateEventMapper[S AggregateState] interface {
	EventsMap() map[string]EventState[S]
}

// InitZeroAggregate initializes an aggregate with the zero state.
func InitZeroAggregate[S AggregateState](state S) *Aggregate[S] {
	return &Aggregate[S]{
		_type:   state.Type(),
		state:   state,
		changes: make([]*Event[S], 0),
	}
}

// InitAggregate initializes an aggregate with the provided id, version, and state.
func InitAggregate[S AggregateState](id string, version int, state S) *Aggregate[S] {
	return &Aggregate[S]{
		id:      id,
		version: version,
		_type:   state.Type(),
		state:   state,
	}
}

// Aggregate represents an aggregate in event sourcing.
type Aggregate[S AggregateState] struct {
	id         string
	version    int
	_type      string
	state      S
	changes    []*Event[S]
	metadata  Metadata
	Invariants []func(*Aggregate[S]) error
}

// Clone creates a deep copy of the Aggregate.
func (a *Aggregate[S]) Clone() *Aggregate[S] {
	var clone S

	m, _ := json.Marshal(a.state)
	_ = json.Unmarshal(m, &clone)

	return &Aggregate[S]{
		id:      a.id,
		version: a.version,
		_type:   a.state.Type(),
		state:   clone,
	}
}

// Metadata returns the metadata of the aggregate.
func (a *Aggregate[S]) Metadata() Metadata {
	return a.metadata
}

// MustAlso appends given invariants to the existing list of invariants.
func (a *Aggregate[S]) MustAlso(ii ...func(*Aggregate[S]) error) {
	a.Invariants = append(a.Invariants, ii...)
}

// Must replaces the current invariants with the provided ones.
func (a *Aggregate[S]) Must(ii ...func(*Aggregate[S]) error) {
	a.Invariants = ii
}

// Check checks all invariants of the aggregate and returns the first error encountered. Check returns nil if all
// Invariants passed without failing.
func (a *Aggregate[S]) Check() error {
	for _, inv := range a.Invariants {
		if err := inv(a); err != nil {
			return err
		}
	}

	return nil
}

// CheckAll checks all invariants of the aggregate and returns errors for every failed Invariants. CheckAll returns nil
// if all Invariants passed without failing.
func (a *Aggregate[S]) CheckAll() []error {
	errs := make([]error, 0)

	for _, inv := range a.Invariants {
		if err := inv(a); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}

// Changes returns all changes/events associated with the aggregate.
func (a *Aggregate[S]) Changes() []*Event[S] {
	return a.changes
}

// ID returns the ID of the aggregate.
func (a *Aggregate[S]) ID() string {
	return a.id
}

// Type returns the type of the aggregate.
func (a *Aggregate[S]) Type() string {
	return a._type
}

// Version returns the version of the aggregate.
func (a *Aggregate[S]) Version() int {
	return a.version
}

func (a *Aggregate[S]) incrementVersion() int {
	a.version++

	return a.version
}

// State returns the current state of the aggregate.
func (a *Aggregate[S]) State() S {
	return a.state
}

// MarshalJSON marshals the aggregate into JSON format.
func (a *Aggregate[S]) MarshalJSON() ([]byte, error) {
	type J struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
		Type    string `json:"type"`
		State   S      `json:"state"`
	}

	res := J{
		ID:      a.id,
		Version: a.version,
		Type:    a._type,
		State:   a.state,
	}

	return json.Marshal(&res)
}
