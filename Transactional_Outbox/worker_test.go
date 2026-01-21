package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var db *sql.DB

// TestMain handles the expensive Docker startup ONCE
func TestMain(m *testing.M) {
	ctx := context.Background()

	// 1. Spin up Postgres Docker Container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_PASSWORD": "password"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections"),
	}
	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(err)
	}
	defer postgresC.Terminate(ctx)

	// 2. Connect to it
	host, _ := postgresC.Host(ctx)
	port, _ := postgresC.MappedPort(ctx, "5432")
	connStr := "postgres://postgres:password@" + host + ":" + port.Port() + "/postgres?sslmode=disable"

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	// 3. Run Migrations (Create the table)
	// We use the schema we defined in schema.sql
	_, err = db.Exec(`
		CREATE TABLE outbox (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			payload JSONB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INT DEFAULT 0,
			next_attempt_at TIMESTAMP DEFAULT NOW(),
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		);
	`)
	if err != nil {
		panic(err)
	}

	// 4. Run Tests
	m.Run()
}

func TestWorkerProcessesEmail(t *testing.T) {
	// 1. Setup Data (Truncate to ensure clean state)
	if _, err := db.Exec("TRUNCATE outbox"); err != nil {
		t.Fatal(err)
	}

	// 2. Insert a pending job
	// Note: Postgres gen_random_uuid() generates the ID if we don't supply it,
	// but supplying it makes asserting easier.
	jobID := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	_, err := db.Exec("INSERT INTO outbox (id, payload, status) VALUES ($1, '{}', 'pending')", jobID)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Start Worker in Background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := WorkerConfig{
		DB:          db,
		Limiter:     &MockLimiter{Allowed: true}, // Simple mock just for this test
		Metrics:     &MockMetrics{},
		EmailClient: func(m OutboxMsg) error { return nil }, // Always succeeds
		MaxRate:     10,
	}
	go RunWorker(ctx, cfg)

	// 4. ASSERTION: The "Eventually" pattern
	// We do NOT sleep. We poll the DB until the state changes.
	assert.Eventually(t, func() bool {
		var status string
		err := db.QueryRow("SELECT status FROM outbox WHERE id = $1", jobID).Scan(&status)
		return err == nil && status == "sent"
	}, 5*time.Second, 100*time.Millisecond, "Job should be processed and marked sent")
}

// Minimal mocks to satisfy interfaces
type MockLimiter struct{ Allowed bool }

func (m *MockLimiter) Allow(_ context.Context, _ string, _ int, _ time.Duration) (bool, time.Duration) {
	return m.Allowed, 0
}

type MockMetrics struct{}

func (m *MockMetrics) Gauge(_ string, _ float64)     {}
func (m *MockMetrics) Histogram(_ string, _ float64) {}
