# go-toolkit-flow

A generic data pipeline toolkit for Go. Build concurrent data processing flows with pluggable storage backends.

## Architecture

```
Source → [Group: Runner(processor) → Destination] → ... → Done
```

- **Source**: reads data in pages, streams batches via channel
- **Processor**: transforms data batch-by-batch (consumer path: no output; producer path: produces output)
- **Destination**: writes output data in bulk
- **Flow**: orchestrates lifecycle — Prepare → Scan → Process → Finish → Close

A flow supports multiple groups (fan-out), each with one or more runners and one or more destinations.

## Installation

```bash
go get github.com/auho/go-toolkit-flow/v3
```

Requires Go 1.26+.

## Quick Start

### Producer Path (source → processor → destination)

```go
package main

import (
    "github.com/auho/go-toolkit-flow/v3/exec"
    "github.com/auho/go-toolkit-flow/v3/exec/producer/item"
    "github.com/auho/go-toolkit-flow/v3/flow"
    "github.com/auho/go-toolkit-flow/v3/processor"
    mockdest "github.com/auho/go-toolkit-flow/v3/storage/mock/destination"
    mocksrc "github.com/auho/go-toolkit-flow/v3/storage/mock/source"
)

// MyProcessor implements producer.Item[MapEntry, MapEntry]
type MyProcessor struct {
    processor.BaseProcessor
}

func (p *MyProcessor) Exec(item map[string]any) ([]map[string]any, bool, error) {
    // transform item and return output
    return []map[string]any{item}, true, nil
}

func main() {
    src, _ := mocksrc.NewMap(mocksrc.Config{Total: 100, PageSize: 10})
    dest, _ := mockdest.NewInsertMap()

    flow.RunFlow[map[string]any, map[string]any](
        flow.WithSource[map[string]any, map[string]any](src),
        flow.WithGroup[map[string]any, map[string]any](
            []exec.Runner[map[string]any, map[string]any]{
                item.NewRunner[map[string]any, map[string]any](&MyProcessor{}),
            },
            dest,
        ),
    )
}
```

### Consumer Path (source → processor, no destination)

```go
flow.RunFlow[map[string]any, map[string]any](
    flow.WithSource[map[string]any, map[string]any](src),
    flow.WithGroup[map[string]any, map[string]any](
        []exec.Runner[map[string]any, map[string]any]{
            batch.NewRunner[map[string]any, map[string]any](&MyConsumerProcessor{}),
        },
        // No destination → defaults to NoopDestination
    ),
)
```

## Supported Entry Types

| Type | Alias | Description |
|------|-------|-------------|
| `MapEntry` | `map[string]any` | General-purpose key-value map |
| `SliceEntry` | `[]any` | Positional slice |
| `StringSliceEntry` | `[]string` | String-only positional slice |
| `StringMapEntry` | `map[string]string` | String-only key-value map |
| `ScoreMapEntry` | `map[any]float64` | Redis sorted set member → score |
| `string` | — | Plain string |

## Storage Backends

### Source (data reader)

| Backend | Entry Point | Entry Type |
|---------|-------------|------------|
| Mock | `mocksrc.NewMap(cfg)` | `MapEntry` |
| Mock | `mocksrc.NewSlice(cfg)` | `SliceEntry` |
| Mock | `mocksrc.NewString(cfg)` | `string` |
| Mock | `mocksrc.NewStringMap(cfg)` | `StringMapEntry` |
| MySQL | `dbsrc.NewSectionMapWithGorm(secCfg, scanCfg, db)` | `MapEntry` |
| Redis | `redissrc.NewHashesWithGoRedisV8(client, cfg)` | `StringMapEntry` |
| Redis | `redissrc.NewListsWithGoRedisV8(client, cfg)` | `string` |
| Redis | `redissrc.NewSetsWithGoRedisV8(client, cfg)` | `string` |
| Redis | `redissrc.NewSortedSetsWithGoRedisV8(client, cfg)` | `StringMapEntry` |
| Redis | `redissrc.NewScanWithGoRedisV8(client, cfg)` | `string` |
| Redis | `redissrc.NewHashesWithGoRedisV9(client, cfg)` | `StringMapEntry` |
| Redis | `redissrc.NewListsWithGoRedisV9(client, cfg)` | `string` |
| Redis | `redissrc.NewSetsWithGoRedisV9(client, cfg)` | `string` |
| Redis | `redissrc.NewSortedSetsWithGoRedisV9(client, cfg)` | `StringMapEntry` |
| Redis | `redissrc.NewScanWithGoRedisV9(client, cfg)` | `string` |
| File | `filesrc.NewLine(cfg)` | `string` |

### Destination (data writer)

| Backend | Entry Point | Entry Type |
|---------|-------------|------------|
| Mock | `mockdest.NewInsertMap()` | `MapEntry` |
| Mock | `mockdest.NewInsertSlice()` | `SliceEntry` |
| Mock | `mockdest.NewUpdateMap()` | `MapEntry` |
| MySQL | `dbdest.NewBulkInsertMapWithGorm(cfg, wc, db)` | `MapEntry` |
| MySQL | `dbdest.NewBulkInsertSliceWithGorm(cfg, wc, fields, db)` | `SliceEntry` |
| MySQL | `dbdest.NewBulkUpdateMapWithGorm(cfg, wc, idName, db)` | `MapEntry` |
| Redis | `redisdest.NewHashesWithGoRedisV8(client, cfg)` | `MapEntry` |
| Redis | `redisdest.NewListsWithGoRedisV8(client, cfg)` | `string` |
| Redis | `redisdest.NewSetsWithGoRedisV8(client, cfg)` | `string` |
| Redis | `redisdest.NewSortedSetsWithGoRedisV8(client, cfg)` | `ScoreMapEntry` |
| Redis | `redisdest.NewHashesWithGoRedisV9(client, cfg)` | `MapEntry` |
| Redis | `redisdest.NewListsWithGoRedisV9(client, cfg)` | `string` |
| Redis | `redisdest.NewSetsWithGoRedisV9(client, cfg)` | `string` |
| Redis | `redisdest.NewSortedSetsWithGoRedisV9(client, cfg)` | `ScoreMapEntry` |
| File | `filedest.NewLine(cfg)` | `string` |

## Processor Adapters

| Path | Granularity | Adapter | Package |
|------|-------------|---------|---------|
| Consumer | Batch | `consumer/batch.NewRunner(b)` | `exec/consumer/batch` |
| Consumer | Item | `consumer/item.NewRunner(it)` | `exec/consumer/item` |
| Producer | Batch | `producer/batch.NewRunner(b)` | `exec/producer/batch` |
| Producer | Item | `producer/item.NewRunner(it)` | `exec/producer/item` |

Embed `processor.BaseProcessor` for zero-value state/output/log management.

## Module Structure

```
flow/           Pipeline orchestration (RunFlow, WithSource, WithGroup)
exec/           Runner lifecycle, executor adapters
processor/      Processor interface and BaseProcessor
storage/        Source/Destination contracts and entry types
  database/     MySQL source (Section) and destination (Bulk) via GORM
  redis/        Redis source (Iterator) and destination (Bulk) via go-redis
  file/         File source and destination (line-based)
  mock/         In-memory source/destination for testing
  tool/         Deep copy utilities for entry types
```

## License

See [LICENSE](LICENSE).
