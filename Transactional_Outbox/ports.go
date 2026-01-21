package main

import (
	"context"
	"time"
)

type RateLimiter interface {
	// Allow checks if we can proceed. If not, it returns false and a wait duration.
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration)
}

type MetricClient interface {
	Gauge(name string, value float64)
	Histogram(name string, duration float64)
}
