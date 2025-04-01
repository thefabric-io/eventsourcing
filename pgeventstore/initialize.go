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

		if err := ensureOffsetColumn(tx, schema, t); err != nil {
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

func ensureOffsetColumn(tx *sqlx.Tx, schema, tableName string) error {
	// Check if column exists
	var exists bool
	query := `
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = $1
            AND table_name = $2 
            AND column_name = $3
        );`
	err := tx.QueryRow(query, schema, tableName, "offset").Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		// Create the column as nullable first
		if _, err := tx.Exec(fmt.Sprintf(`
            ALTER TABLE %s.%s ADD COLUMN "offset" BIGSERIAL;
        `, schema, tableName)); err != nil {
			return err
		}

		// Populate existing records
		if _, err := tx.Exec(fmt.Sprintf(`
            WITH numbered_events AS (
                SELECT id, 
                    ROW_NUMBER() OVER (ORDER BY occurred_at, registered_at) as row_num
                FROM %s.%s
            )
            UPDATE %s.%s t
            SET "offset" = n.row_num
            FROM numbered_events n
            WHERE t.id = n.id;
        `, schema, tableName, schema, tableName)); err != nil {
			return err
		}

		// Make it non-nullable after population
		if _, err := tx.Exec(fmt.Sprintf(`
            ALTER TABLE %s.%s ALTER COLUMN "offset" SET NOT NULL;
        `, schema, tableName)); err != nil {
			return err
		}
	}
	return nil
}
