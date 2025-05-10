package pgeventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"regexp"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/thefabric-io/eventsourcing"
)

const maxBatchSize = 500

func Storage[S eventsourcing.AggregateState]() eventsourcing.EventStore[S] {
	var aggregateState S

	return &storage[S]{
		aggregateState: aggregateState,
	}
}

type storage[S eventsourcing.AggregateState] struct {
	aggregateState S
}

func (s *storage[S]) Load(ctx context.Context, transaction eventsourcing.Transaction, aggregateID string, version eventsourcing.AggregateVersion) (*eventsourcing.Aggregate[S], error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`select
		id, occurred_at, registered_at, type,
		aggregate_id, aggregate_version, aggregate_type,
		aggregate_state, state, "offset", metadata
	from %s.%s where aggregate_id = $1 and aggregate_type = $2`, schema, eventsourcing.StorageName(s.aggregateState)))

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
		return nil, eventsourcing.ErrAggregateNotFound
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
	from %s.%s where aggregate_id = $1 and aggregate_type = $2`, schema, eventsourcing.StorageName(s.aggregateState)))

	values := make([]any, 0)
	values = append(values, aggregateID, eventsourcing.StorageName(s.aggregateState))

	if fromVersion != eventsourcing.LastVersion {
		sb.WriteString(" and aggregate_version >= $3 ")
		values = append(values, fromVersion)
	} else {
		sb.WriteString(fmt.Sprintf(` and aggregate_version = 
												(select max(aggregate_version) from %s.%s 
												where aggregate_id = $1 and aggregate_type = $2)`, schema, eventsourcing.StorageName(s.aggregateState)))
	}

	sb.WriteString(fmt.Sprintf(" limit %d ", limit))

	tx := transaction.(*sqlx.Tx)

	var events []event
	err := tx.SelectContext(ctx, &events, sb.String(), values...)
	if err != nil {
		return nil, err
	}

	res, err := s.parseEvents(events)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *storage[S]) Events(ctx context.Context, transaction eventsourcing.Transaction, p eventsourcing.EventsParams) ([]*eventsourcing.Event[S], error) {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`select
		id, occurred_at, registered_at, type,
		aggregate_id, aggregate_version, aggregate_type,
		aggregate_state, state, "offset", metadata
	from %s.%s where true and "offset" > $1 and type = any($2)`, schema, eventsourcing.StorageName(s.aggregateState)))

	values := make([]any, 0)
	values = append(values, p.AfterOffset)

	if p.TypesOnly != nil {
		values = append(values, pq.Array(p.TypesOnly))
	}

	if p.Limit == 0 {
		p.Limit = 1000
	}

	sb.WriteString(fmt.Sprintf(` order by "offset" asc limit %d`, p.Limit))

	tx := transaction.(*sqlx.Tx)

	var events []event
	err := tx.SelectContext(ctx, &events, sb.String(), values...)
	if err != nil {
		return nil, err
	}

	res, err := s.parseEvents(events)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *storage[S]) Save(ctx context.Context, transaction eventsourcing.Transaction, aggregate ...*eventsourcing.Aggregate[S]) error {
	events := make([]*eventsourcing.Event[S], 0)
	knownAggregates := make(map[string]bool)

	for _, a := range aggregate {
		if _, ok := knownAggregates[a.ID()]; !ok {
			events = append(events, a.Changes()...)
			knownAggregates[a.ID()] = true
		}
	}

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
	) values `, schema, eventsourcing.StorageName(s.aggregateState))

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

	_, err := tx.Exec(query, values...)

	return err
}

func (s *storage[S]) parseEvents(events []event) ([]*eventsourcing.Event[S], error) {
	res := make([]*eventsourcing.Event[S], len(events))

	for i, e := range events {
		var aggregateState S

		if err := json.Unmarshal(e.AggregateState, &aggregateState); err != nil {
			return nil, err
		}

		aggregate := eventsourcing.InitAggregate[S](e.ID.String, int(e.AggregateVersion.Int64), aggregateState)

		aggregateEventMapper, ok := any(aggregate.State()).(eventsourcing.AggregateStateEventMapper[S])
		if !ok {
			return nil, errors.New(fmt.Sprintf("cannot parse event type '%s' for '%s' aggregate: aggregate does not implement eventsourcing.AggregateStateEventMapper[S]", e.Type.String, aggregate.Type()))
		}

		evtType, exists := aggregateEventMapper.EventsMap()[e.Type.String]
		if !exists {
			return nil, errors.New(fmt.Sprintf("cannot parse event type '%s' for '%s' aggregate, the event type is not found: verify that your are parsing the event correctly in your aggregate state EventsMap() map[string]EventState[S] method", e.Type.String, aggregate.Type()))
		}

		evt, err := eventsourcing.InitEvent(e.ID.String, e.OccurredAt.Time, evtType, aggregate, e.State, int(e.Offset.Int64), e.Metadata)
		if err != nil {
			return nil, err
		}

		res[i] = evt
	}

	return res, nil
}
