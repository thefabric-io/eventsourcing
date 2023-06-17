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

func init() {
	_ = godotenv.Load()

	pgURL := os.Getenv("EVENT_STORE_PG_URL")
	if pgURL == "" {
		panic(errors.New("database connection string is required in your environment variable definition under the key 'EVENT_STORE_PG_URL'"))
	}

	db, err := sqlx.Connect("postgres", pgURL)
	if err != nil {
		panic(err)
	}

	envSchema := os.Getenv("EVENT_STORE_SCHEMA")
	if envSchema != "" {
		schema = envSchema
	}

	b := databaseBuilder{
		db: db,
	}

	tx, err := db.Beginx()
	if err != nil {
		panic(err)
	}

	defer tx.Rollback()

	aggregates := strings.Split(os.Getenv("EVENT_STORE_AGGREGATES"), ",")

	if err := b.Build(aggregates, tx); err != nil {
		panic(err)
	}

	if err := tx.Commit(); err != nil {
		panic(err)
	}
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
