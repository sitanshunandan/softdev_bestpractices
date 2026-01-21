package main

import (
	"context"
	"database/sql"
	"log"
	"time"
)

// OutboxMsg represents a row in our queue
type OutboxMsg struct {
	ID      string
	Payload []byte
}

// WorkerConfig holds our dependencies
type WorkerConfig struct {
	DB          *sql.DB
	Limiter     RateLimiter
	Metrics     MetricClient
	EmailClient func(OutboxMsg) error // Function type for easier testing
	MaxRate     int                   // e.g., 100 emails/sec
}

func RunWorker(ctx context.Context, cfg WorkerConfig) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Metric: Ticker for Queue Depth
	go monitorQueueDepth(ctx, cfg)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processBatch(ctx, cfg)
		}
	}
}

func processBatch(ctx context.Context, cfg WorkerConfig) {
	// 1. GLOBAL RATE LIMIT CHECK (Redis)
	// "email_global" is the shared key across all 50 pods
	allowed, waitTime := cfg.Limiter.Allow(ctx, "email_global", cfg.MaxRate, time.Second)
	if !allowed {
		// If Redis says "Stop", we sleep and skip this cycle.
		// This protects the external API.
		time.Sleep(waitTime)
		return
	}

	// 2. CLAIM CHECK (Same as before)
	rows, err := cfg.DB.QueryContext(ctx, `
        UPDATE outbox
        SET status = 'processing', updated_at = NOW(), attempts = attempts + 1
        WHERE id IN (
            SELECT id FROM outbox
            WHERE status = 'pending' AND next_attempt_at <= NOW()
            ORDER BY created_at ASC
            LIMIT 10
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, payload
    `)
	if err != nil {
		log.Printf("DB Error: %v", err)
		return
	}
	defer rows.Close()

	var messages []OutboxMsg
	for rows.Next() {
		var msg OutboxMsg
		// Note: We need to handle the JSONB payload scanning here.
		// For simplicity in this snippet, we assume scanner compatibility or string handling
		var payloadBytes []byte
		if err := rows.Scan(&msg.ID, &payloadBytes); err != nil {
			log.Printf("Scan Error: %v", err)
			continue
		}
		msg.Payload = payloadBytes
		messages = append(messages, msg)
	}
	rows.Close()

	for _, msg := range messages {
		start := time.Now()

		err := cfg.EmailClient(msg)

		duration := time.Since(start).Seconds()
		cfg.Metrics.Histogram("email_send_duration", duration)

		if err == nil {
			cfg.DB.ExecContext(ctx, "UPDATE outbox SET status = 'sent' WHERE id = $1", msg.ID)
			cfg.Metrics.Gauge("email_success", 1)
		} else {
			// Simple Retry Logic for demo
			cfg.Metrics.Gauge("email_failure", 1)
			cfg.DB.ExecContext(ctx, "UPDATE outbox SET status = 'pending', next_attempt_at = NOW() + INTERVAL '10 seconds' WHERE id = $1", msg.ID)
		}
	}
}

func monitorQueueDepth(ctx context.Context, cfg WorkerConfig) {
	monitorTicker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-monitorTicker.C:
			var count int
			cfg.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE status = 'pending'").Scan(&count)
			cfg.Metrics.Gauge("outbox_queue_depth", float64(count))
		}
	}
}
