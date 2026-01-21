package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

// StdOutMetrics is a simple implementation for local dev
type StdOutMetrics struct{}

func (m *StdOutMetrics) Gauge(name string, value float64) {
	log.Printf("[METRIC] GAUGE %s: %f", name, value)
}
func (m *StdOutMetrics) Histogram(name string, duration float64) {
	log.Printf("[METRIC] HIST %s: %f s", name, duration)
}

func main() {
	// 1. Setup Context with Graceful Shutdown
	// We listen for SIGINT (Ctrl+C) or SIGTERM to stop the worker cleanly.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 2. Connect to Dependencies
	// Note: In a real app, load these from os.Getenv()
	dbConnStr := "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	defer db.Close()

	// Initialize Redis Limiter (Connects to localhost:6379 by default)
	limiter := NewRedisRateLimiter("localhost:6379")

	// 3. Configure the Worker
	cfg := WorkerConfig{
		DB:      db,
		Limiter: limiter,
		Metrics: &StdOutMetrics{},
		MaxRate: 5, // Limit to 5 emails/second
		EmailClient: func(msg OutboxMsg) error {
			// Real Email Logic would go here (e.g., SendGrid SDK)
			log.Printf("📧 Sending email for Job %s...", msg.ID)
			time.Sleep(200 * time.Millisecond) // Simulate network latency
			log.Printf("✅ Email Sent for Job %s", msg.ID)
			return nil
		},
	}

	// 4. Start the Worker
	log.Println("🚀 Outbox Worker starting... (Press Ctrl+C to stop)")
	go RunWorker(ctx, cfg)

	// 5. Wait for Shutdown Signal
	<-sigChan
	log.Println("\n🛑 Shutting down...")
	cancel() // This triggers ctx.Done() in the worker, stopping the loop

	// Give the worker a moment to finish current batch
	time.Sleep(1 * time.Second)
	log.Println("👋 Goodbye.")
}
