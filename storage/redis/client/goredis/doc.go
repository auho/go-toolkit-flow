// Package goredis provides version-specific Redis client wrappers for go-redis.
//
// # Multi-Version Design
//
// go-redis underwent a major version transition: v8 lives at
// github.com/go-redis/redis/v8 (archived), while v9 lives at
// github.com/redis/go-redis/v9 (officially maintained under the Redis
// organization).
//
// To support projects that depend on either version, this package provides
// separate client wrappers:
//
//   - V8 (v8.go): wraps github.com/go-redis/redis/v8
//   - V9 (v9.go): wraps github.com/redis/go-redis/v9
//
// Both wrappers expose the same methods (DB, Close, Truncate, HashLen, etc.)
// so that the source/destination dialect implementations can use either
// version interchangeably. Users choose the wrapper that matches their
// go-redis dependency.
//
// # API Differences (v8 vs v9)
//
// The v9 API is nearly identical to v8:
//   - context.Context remains the first parameter of all commands
//   - redis.Z, redis.NewClient, redis.Options are unchanged
//   - Pipeline method signatures are the same
//
// The key difference is Pipeline concurrency safety:
//
// In v8, Pipeline had an internal sync.Mutex, allowing multiple goroutines
// to safely call pipe.HMSet(), pipe.ZAdd(), etc. on the same pipeline
// instance concurrently.
//
// In v9, this lock was removed for performance — most users use pipelines
// sequentially in a single goroutine, and the mutex added unnecessary
// overhead. If concurrent pipeline access is needed in v9, the caller must
// add external synchronization (e.g., sync.Mutex).
//
// The implementations in this project use pipelines within a single
// goroutine (writeBatch), so the v9 change does not affect correctness.
package goredis
