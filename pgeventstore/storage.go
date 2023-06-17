package pgeventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/thefabric-io/eventsourcing"
)

const maxBatchSize = 500

func Storage[S eventsourcing.AggregateState](parseEventTypeFunc func(t string) (eventsourcing.EventState[S], error)) eventsourcing.EventStore[S] {
	var aggregateState S

	return &storage[S]{
		aggregateState: aggregateState,
		parseEventType: parseEventTypeFunc,
	}
}

type storage[S eventsourcing.AggregateState] struct {
	aggregateState S
	parseEventType func(t string) (eventsourcing.EventState[S], error)
}

func (s *storage[S]) Load(ctx context.Context, transaction eventsourcing.Transaction, aggregateID string, version eventsourcing.AggregateVersion) (*eventsourcing.Aggregate[S], error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`select
		id, occurred_at, registered_at, type,
		aggregate_id, aggregate_version, aggregate_type,
		aggregate_state, state, metadata
	from %s.%s where aggregate_id = $1 and aggregate_type = $2`, schema, s.aggregateState.Type()))

	values := make([]any, 0)
	values = append(values, aggregateID)
	values = append(values, s.aggregateState.Type())
	values = append(values, version)

	if version <= eventsourcing.LastVersion {
		sb.WriteString(" and aggregate_version > $3 ")
	} else {
		sb.WriteString(" and aggregate_version <= $3 ")
	}

	sb.WriteString(" order by aggregate_version desc ")
	sb.WriteString(" limit 1")

	tx := transaction.(*sqlx.Tx)

	var events []event
	err := tx.SelectContext(ctx, &events, sb.String(), values...)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, errors.New(fmt.Sprintf("aggregate with id '%s' and version '%d' cannot be found", aggregateID, version))
	}

	e := events[0]

	var aggregateState S

	aggregate := eventsourcing.InitAggregate[S](e.AggregateID.String, int(e.AggregateVersion.Int64), aggregateState.Zero().(S))

	if err := json.Unmarshal(e.AggregateState, aggregate.State()); err != nil {
		return nil, err
	}

	return aggregate, nil
}

func (s *storage[S]) History(ctx context.Context, transaction eventsourcing.Transaction, aggregateID string, fromVersion int, limit int) ([]*eventsourcing.Event[S], error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`select
		id, occurred_at, registered_at, type,
		aggregate_id, aggregate_version, aggregate_type,
		aggregate_state, state, metadata
	from %s.%s where aggregate_id = $1 and aggregate_type = $2`, schema, s.aggregateState.Type()))

	values := make([]any, 0)
	values = append(values, aggregateID, s.aggregateState.Type())

	if fromVersion != eventsourcing.LastVersion {
		sb.WriteString(" and aggregate_version >= $3 ")
		values = append(values, fromVersion)
	} else {
		sb.WriteString(fmt.Sprintf(` and aggregate_version = 
												(select max(aggregate_version) from %s.%s 
												where aggregate_id = $1 and aggregate_type = $2)`, schema, s.aggregateState.Type()))
	}

	sb.WriteString(fmt.Sprintf(" limit %d ", limit))

	tx := transaction.(*sqlx.Tx)

	var events []event
	err := tx.SelectContext(ctx, &events, sb.String(), values...)
	if err != nil {
		return nil, err
	}

	res := make([]*eventsourcing.Event[S], len(events))

	for i, e := range events {
		var aggregateState S

		if err := json.Unmarshal(e.AggregateState, &aggregateState); err != nil {
			return nil, err
		}

		aggregate := eventsourcing.InitAggregate[S](e.ID.String, int(e.AggregateVersion.Int64), aggregateState)

		evtType, err := s.parseEventType(e.Type.String)
		if err != nil {
			return nil, err
		}

		evt, err := eventsourcing.InitEvent(e.ID.String, e.OccurredAt.Time, evtType, aggregate, e.State, e.Metadata)
		if err != nil {
			return nil, err
		}

		res[i] = evt
	}

	return res, nil
}

func (s *storage[S]) Save(ctx context.Context, transaction eventsourcing.Transaction, aggregate *eventsourcing.Aggregate[S]) error {
	events := aggregate.Changes()

	sqlEvents := make([]*event, len(events))
	for i, e := range events {
		var err error

		sqlEvents[i], err = marshalSQL(e)
		if err != nil {
			return err
		}
	}

	tx := transaction.(*sqlx.Tx)

	if err := s.insertEvents(tx, sqlEvents); err != nil {
		return err
	}

	return nil
}

func (s *storage[S]) insertEvents(tx *sqlx.Tx, ee []*event) error {
	for i := 0; i < len(ee); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(ee) {
			end = len(ee)
		}

		if err := s.insertBatch(tx, ee[i:end]); err != nil {
			return err
		}
	}

	return nil
}

func (s *storage[S]) insertBatch(tx *sqlx.Tx, ee []*event) error {
	query := fmt.Sprintf(`insert into %s.%s(
		id, occurred_at, registered_at, type, aggregate_id, aggregate_version, aggregate_type, aggregate_state, state, metadata
	) values `, schema, s.aggregateState.Type())

	values := make([]any, 0)
	for _, e := range ee {
		query += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?),"
		values = append(values, e.ID, e.OccurredAt, e.RegisteredAt, e.Type, e.AggregateID, e.AggregateVersion, e.AggregateType, e.AggregateState, e.State, e.Metadata)
	}

	query = query[:len(query)-1]

	re := regexp.MustCompile(`\?`)

	var n int

	query = re.ReplaceAllStringFunc(query, func(string) string {
		n++
		return "$" + strconv.Itoa(n)
	})

	tx.Preparex(query)
	_, err := tx.Exec(query, values...)

	return err
}
