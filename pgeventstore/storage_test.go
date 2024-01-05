package pgeventstore

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

const PostgresPort = 45432

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	opts := &dockertest.RunOptions{
		Name:         "test_eventsourcing_postgres",
		Repository:   "postgres",
		Tag:          "latest",
		ExposedPorts: []string{"5432/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {
				{
					HostIP:   "localhost",
					HostPort: fmt.Sprintf("%d/tcp", PostgresPort),
				},
			},
		},
		Env: []string{
			"POSTGRES_USER=default",
			"POSTGRES_PASSWORD=default",
		},
	}

	resource, err := pool.RunWithOptions(opts, func(config *docker.HostConfig) {
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		config.AutoRemove = true
	})
	if err != nil {
		if !errors.Is(err, docker.ErrContainerAlreadyExists) {
			log.Fatalf("Could not start resource: %s", err)
		}

		// start existing instance
		var ok bool
		if resource, ok = pool.ContainerByName("test_eventsourcing_postgres"); ok {
			if !resource.Container.State.Running {
				pool.Client.StartContainer(resource.Container.ID, nil)
			}
		}

		if resource == nil {
			log.Fatalf("Could not start resource: %s", err)
		}
	}

	resource.Expire(uint(time.Minute.Seconds()))

	// exponential backoff-retry
	if err := pool.Retry(func() error {
		conn := fmt.Sprintf("postgres://default:default@localhost:%d?sslmode=disable", PostgresPort)

		db, err := sql.Open("postgres", conn)
		if err != nil {
			return err
		}
		defer db.Close()

		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	// To speed up tests initialization, you don't have to purge
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func Test_Init(t *testing.T) {
	t.Parallel()

	config := EventStorageConfig{
		PostgresURL: fmt.Sprintf("postgres://default:default@localhost:%d?sslmode=disable", PostgresPort),
		Schema:      "config",
		Aggregates:  "aggregate",
	}

	// init event storage with config
	if err := Init(config); err != nil {
		t.Fatal(err)
	}

	// simple test to check whether the schema and aggregate table have been created
	db, err := sqlx.Connect("postgres", config.PostgresURL)
	if err != nil {
		t.Fatal(err)
	}

	var query = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = $1 AND tablename = $2)"

	var exists bool
	if err := db.Get(&exists, query, config.Schema, config.Aggregates); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("no schema or aggregated table has been created")
	}

	os.Setenv("EVENT_STORE_PG_URL", config.PostgresURL)
	os.Setenv("EVENT_STORE_SCHEMA", "env")
	os.Setenv("EVENT_STORE_AGGREGATES", config.Aggregates)

	// init event storage with env variables
	if err := Init(); err != nil {
		t.Fatal(err)
	}

	if err := db.Get(&exists, query, "env", config.Aggregates); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("no schema or aggregated table has been created")
	}
}
