package pgeventconsumer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/thefabric-io/eventsourcing/eventconsumer"
	"github.com/thefabric-io/transactional"
)

func PostgresConsumerStore() eventconsumer.ConsumerStore {
	return &consumerStore{}
}

type consumerStore struct{}

func (s *consumerStore) Save(ctx context.Context, transaction transactional.Transaction, name string, offsetAcked, offsetConsumed int) error {
	query := `
		INSERT INTO eventstore.events_consumers 
			(consumer_group, aggregate_type, offset_acked, offset_consumed) 
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (consumer_group, aggregate_type) 
		DO UPDATE SET 
			offset_acked = EXCLUDED.offset_acked, 
			offset_consumed = EXCLUDED.offset_consumed,
			last_occurred_at = now();
	`

	tx := transaction.(*sqlx.Tx)
	if _, err := tx.ExecContext(ctx, query, name, "", offsetAcked, offsetConsumed); err != nil {
		return err
	}

	return nil
}

func (s *consumerStore) Load(ctx context.Context, transaction transactional.Transaction, name string) (eventconsumer.Consumer, error) {
	query := fmt.Sprintf(`
		select consumer_group, aggregate_type, offset_acked, offset_consumed, last_occurred_at 
		from eventstore.events_consumers where consumer_group = $1;`)

	var result Consumer

	tx := transaction.(*sqlx.Tx)

	if err := tx.GetContext(ctx, &result, query, name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := s.Save(ctx, tx, name, 0, 0); err != nil {
				return nil, err
			}

			return s.Load(ctx, tx, name)
		} else {
			return nil, err
		}
	}

	return &result, nil
}

type Consumer struct {
	Group_          sql.Null[string]    `db:"consumer_group"`
	AggregateType_  sql.Null[string]    `db:"aggregate_type"`
	OffsetAcked_    sql.Null[int]       `db:"offset_acked"`
	OffsetConsumed_ sql.Null[int]       `db:"offset_consumed"`
	LastOccurred_   sql.Null[time.Time] `db:"last_occurred_at"`
}

func (c Consumer) Group() string {
	return c.Group_.V
}

func (c Consumer) AggregateType() string {
	return c.AggregateType_.V
}

func (c Consumer) OffsetAcked() int {
	return c.OffsetAcked_.V
}

func (c Consumer) OffsetConsumed() int {
	return c.OffsetConsumed_.V
}

func (c Consumer) LastOccurred() *time.Time {
	if c.LastOccurred_.Valid {
		return &c.LastOccurred_.V
	}

	return nil
}
