package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRateLimiter struct {
	Client *redis.Client
}

func NewRedisRateLimiter(addr string) *RedisRateLimiter {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisRateLimiter{Client: rdb}
}

// Allow implements the Token Bucket algorithm (simplified fixed window) using Redis
func (r *RedisRateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration) {
	// 1. Increment the counter for this time window
	// We use the current second/minute as part of the key to "bucket" time
	// For a sliding window, we'd use ZSET, but for this tutorial, we use a simple fixed window with expiration.

	// Increment: If key doesn't exist, it is set to 1.
	count, err := r.Client.Incr(ctx, key).Result()
	if err != nil {
		// Fail open or closed? Here we fail open (allow) to avoid blocking traffic if Redis is down,
		// but log the error in a real app.
		return true, 0
	}

	// 2. Set Expiration (only on the first increment)
	if count == 1 {
		r.Client.Expire(ctx, key, window)
	}

	// 3. Check Limit
	if count > int64(limit) {
		// Calculate time until the key expires (reset)
		ttl, _ := r.Client.TTL(ctx, key).Result()
		return false, ttl
	}

	return true, 0
}
