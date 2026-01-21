package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// OutboxMsg represents a row in our queue
type OutboxMsg struct {
	ID      string
	Payload []byte
}

// ---------------------------------------------------------
// 1. The Transactional Insert (The "Producer")
// ---------------------------------------------------------

// CreateOrder atomically saves the order and the email intent.
func CreateOrder(ctx context.Context, db *sql.DB, orderID string, email string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // Safety net

	// Step A: Insert the Order (Business Logic)
	// _, err = tx.ExecContext(ctx, "INSERT INTO orders (id, email) VALUES ($1, $2)", orderID, email)
	// if err != nil { return err }

	// Step B: Insert the Outbox Message (The Intent)
	payload, _ := json.Marshal(map[string]string{
		"order_id": orderID,
		"email":    email,
		"subject":  "Order Confirmed",
	})

	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (payload, status, next_attempt_at)
		VALUES ($1, 'pending', NOW())
	`, payload)
	if err != nil {
		return err
	}

	// Commit: Both happen, or neither happens.
	return tx.Commit()
}

// ---------------------------------------------------------
// 2. The Worker (The "Consumer")
// ---------------------------------------------------------

func RunWorker(ctx context.Context, db *sql.DB) {
	ticker := time.NewTicker(100 * time.Millisecond) // Poll interval
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker shutting down...")
			return
		case <-ticker.C:
			// Run the processing loop
			if err := processBatch(ctx, db); err != nil {
				log.Printf("Error processing batch: %v", err)
			}
		}
	}
}

func processBatch(ctx context.Context, db *sql.DB) error {
	// -----------------------------------------------------
	// Phase 1: CLAIM CHECK
	// Identify available work, mark it "processing", and
	// RELEASE the DB connection immediately.
	// -----------------------------------------------------

	rows, err := db.QueryContext(ctx, `
		UPDATE outbox
		SET status = 'processing',
		    updated_at = NOW(),
		    attempts = attempts + 1
		WHERE id IN (
			SELECT id FROM outbox
			WHERE status = 'pending'
			  AND next_attempt_at <= NOW()
			ORDER BY created_at ASC
			LIMIT 10
			FOR UPDATE SKIP LOCKED -- The magic concurrency sauce
		)
		RETURNING id, payload
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var messages []OutboxMsg
	for rows.Next() {
		var msg OutboxMsg
		if err := rows.Scan(&msg.ID, &msg.Payload); err != nil {
			return err
		}
		messages = append(messages, msg)
	}
	rows.Close() // Explicitly close to ensure connection release

	if len(messages) == 0 {
		return nil // No work found
	}

	log.Printf("Claimed %d messages. Processing...", len(messages))

	// -----------------------------------------------------
	// Phase 2: PROCESSING (Network Heavy)
	// We are NOT holding a DB lock here.
	// -----------------------------------------------------

	for _, msg := range messages {
		// Simulate slow Email API
		err := SendEmail(msg)

		// -------------------------------------------------
		// Phase 3: SETTLEMENT
		// Re-acquire DB connection to mark result
		// -------------------------------------------------
		if err == nil {
			// Success! Mark as sent.
			_, _ = db.ExecContext(ctx, `
				UPDATE outbox SET status = 'sent', updated_at = NOW() 
				WHERE id = $1`, msg.ID)
		} else {
			// Failure! Backoff strategy.
			// If attempts > 5, maybe move to a DLQ table instead.
			log.Printf("Failed to send %s: %v", msg.ID, err)
			_, _ = db.ExecContext(ctx, `
				UPDATE outbox 
				SET status = 'pending', 
				    next_attempt_at = NOW() + (POWER(2, attempts) * INTERVAL '1 second'),
				    updated_at = NOW()
				WHERE id = $1`, msg.ID)
		}
	}

	return nil
}

// ---------------------------------------------------------
// 3. The Safety Net (Rescue Stuck Jobs)
// ---------------------------------------------------------

// RescueStuckJobs runs periodically to find jobs that crashed
// while in 'processing' state.
func RescueStuckJobs(ctx context.Context, db *sql.DB) {
	_, err := db.ExecContext(ctx, `
		UPDATE outbox
		SET status = 'pending',
		    next_attempt_at = NOW()
		WHERE status = 'processing'
		  AND updated_at < NOW() - INTERVAL '5 minutes'
	`)
	if err != nil {
		log.Printf("Error rescuing stuck jobs: %v", err)
	}
}

// ---------------------------------------------------------
// Mock Email Client
// ---------------------------------------------------------
func SendEmail(msg OutboxMsg) error {
	// Logic to call SendGrid/AWS SES goes here
	time.Sleep(500 * time.Millisecond) // Simulate latency
	return nil
}

func main() {
	// Setup DB (Placeholder)
	db, _ := sql.Open("postgres", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")

	// Start the worker in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RunWorker(ctx, db)

	// Keep main alive
	select {}
}
