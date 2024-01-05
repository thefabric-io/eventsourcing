package pgeventstore

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var schema = "eventsourcing"

type EventStorageConfig struct {
	PostgresURL string
	Schema      string
	Aggregates  string
}

func Init(optionalConfig ...EventStorageConfig) error {
	if len(optionalConfig) == 0 {
		_ = godotenv.Load()

		optionalConfig = append(optionalConfig, EventStorageConfig{
			PostgresURL: os.Getenv("EVENT_STORE_PG_URL"),
			Schema:      os.Getenv("EVENT_STORE_SCHEMA"),
			Aggregates:  os.Getenv("EVENT_STORE_AGGREGATES"),
		})
	} else if len(optionalConfig) > 1 {
		return errors.New("only one config is allowed")
	}

	config := optionalConfig[0]

	if config.PostgresURL == "" {
		return errors.New("database connection string is required")
	}

	db, err := sqlx.Connect("postgres", config.PostgresURL)
	if err != nil {
		return err
	}

	if config.Schema != "" {
		schema = config.Schema
	}

	b := databaseBuilder{
		db: db,
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err := b.Build(strings.Split(config.Aggregates, ","), tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

type databaseBuilder struct {
	db *sqlx.DB
}

func (b *databaseBuilder) Build(aggregates []string, tx *sqlx.Tx) error {
	if err := b.createSchema(tx); err != nil {
		return err
	}

	tables := deleteEmpty(aggregates)
	if len(tables) == 0 {
		fmt.Println("aggregates zero")
		return nil
	}

	for _, t := range tables {
		if err := b.createTables(t, tx); err != nil {
			return err
		}
	}

	return nil
}

func deleteEmpty(ss []string) []string {
	var r []string
	for _, str := range ss {
		if str != "" {
			r = append(r, str)
		}
	}

	return r
}

func (b *databaseBuilder) createSchema(tx *sqlx.Tx) error {
	query := fmt.Sprintf("create schema if not exists %s;", schema)

	if _, err := tx.Exec(query); err != nil {
		return err
	}

	return nil
}

func (b *databaseBuilder) createTables(name string, tx *sqlx.Tx) error {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`create table if not exists %s.%s(
				id varchar primary key,
				occurred_at timestamptz,
				registered_at timestamptz default now(),
				type varchar,
				state jsonb,
				aggregate_id varchar,
				aggregate_version integer,
				aggregate_type varchar,
				aggregate_state jsonb,
				metadata jsonb
			);`, schema, name))

	sb.WriteString(fmt.Sprintf(`create index if not exists idx_events_aggregate_id_version_type 
			    on %s.%s (aggregate_id, aggregate_version, aggregate_type);`, schema, name))

	sb.WriteString(fmt.Sprintf(`create index if not exists idx_events_aggregate_id_type 
			    on %s.%s (aggregate_id, aggregate_type);`, schema, name))

	if _, err := tx.Exec(sb.String()); err != nil {
		return err
	}

	return nil
}
