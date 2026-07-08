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
// Both wrappers implement the RedisClient interface (common.go), which
// exposes all Redis commands needed by the source/destination dialects.
// The dialect implementations use RedisClient rather than V8/V9 directly,
// so dialect logic is written once and works with either go-redis version.
// Users choose the wrapper that matches their go-redis dependency.
//
// # API Differences (v8 vs v9)
//
// The v9 API is nearly identical to v8:
//   - context.Context remains the first parameter of all commands
//   - redis.Z, redis.NewClient, redis.Options are unchanged
//   - Pipeline method signatures are the same
//
// The key differences handled by the wrappers are:
//
//   - Pipeline concurrency safety: v8 Pipeline had an internal sync.Mutex;
//     v9 removed it. The implementations use pipelines within a single
//     goroutine, so this does not affect correctness.
//   - pipe.Close(): present in v8 Pipeliner, removed in v9. The V8 wrapper
//     calls pipe.Close() after pipe.Exec(); the V9 wrapper does not.
//   - ZAdd argument: v8 accepts *redis.Z (pointer), v9 accepts redis.Z (value).
package goredis
