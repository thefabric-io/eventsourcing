package pgeventstore

import (
	"database/sql"
	"encoding/json"
	"github.com/thefabric-io/eventsourcing"
	"time"
)

// Update the event struct to include Offset
type event struct {
	ID               sql.NullString  `db:"id"`
	Offset           sql.NullInt64   `db:"offset"` // New field
	OccurredAt       sql.NullTime    `db:"occurred_at"`
	RegisteredAt     sql.NullTime    `db:"registered_at"`
	Type             sql.NullString  `db:"type"`
	AggregateID      sql.NullString  `db:"aggregate_id"`
	AggregateType    sql.NullString  `db:"aggregate_type"`
	AggregateVersion sql.NullInt64   `db:"aggregate_version"`
	Offset           sql.NullInt64   `db:"offset"`
	AggregateState   json.RawMessage `db:"aggregate_state"`
	State            json.RawMessage `db:"state"`
	Metadata         json.RawMessage `db:"metadata"`
}

func marshalSQL[S eventsourcing.AggregateState](e *eventsourcing.Event[S]) (*event, error) {
	aggregateState, err := json.Marshal(e.Aggregate().State())
	if err != nil {
		return nil, err
	}

	eventState, err := json.Marshal(e.State())
	if err != nil {
		return nil, err
	}

	metadata, err := json.Marshal(e.Metadata())
	if err != nil {
		return nil, err
	}

	return &event{
		ID:               sqlString(e.ID(), true),
		OccurredAt:       sqlTime(e.OccurredAt(), !e.OccurredAt().IsZero()),
		RegisteredAt:     sqlTime(time.Now(), true),
		Type:             sqlString(e.Type(), true),
		AggregateID:      sqlString(e.Aggregate().ID(), true),
		AggregateVersion: sqlInt64(e.Aggregate().Version(), true),
		AggregateType:    sqlString(e.Aggregate().Type(), true),
		AggregateState:   sqlBytes(aggregateState, aggregateState != nil),
		State:            sqlBytes(eventState, eventState != nil),
		Metadata:         sqlBytes(metadata, metadata != nil),
	}, nil
}

func sqlString(s string, valid bool) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  valid,
	}
}

func sqlInt64(i int, valid bool) sql.NullInt64 {
	return sql.NullInt64{
		Int64: int64(i),
		Valid: valid,
	}
}

func sqlBytes(b []byte, valid bool) []byte {
	if !valid {
		return []byte(`[]`)
	}

	return b
}

func sqlTime(t time.Time, valid bool) sql.NullTime {
	return sql.NullTime{
		Time:  t,
		Valid: valid,
	}
}
