// Package flow is the top-level orchestration layer that wires together
// Source, Runner, and Destination components into a data processing pipeline.
//
// # Data Flow
//
// Data flows from Source through Runners to Destination:
//
//	Source -> transport(fan-out) -> [group1.runners, group2.runners, ...]
//	                                 ↓ executor.Exec (SE -> DE)
//	                           [runner.OutChan, ...]
//	                                 ↓ per-group fan-in
//	                           group.destination.Receive
//	                                 ↓ (MultiDestination fan-out)
//	                           [sub-dest1, sub-dest2, ...]
//
// Each group runs independently. Runners within a group receive the same
// input (fan-out), and their outputs are merged (fan-in) before forwarding
// to the group's destination. Groups execute concurrently.
//
// # Lifecycle Phases
//
// The flow executes in five phases:
//
//	Phase 1: Prepare (synchronous, fail-fast)
//	  source.Prepare(asyncCtx) -> groups.Prepare(asyncCtx, rootCtx)
//	  Errors surface before any goroutines start.
//
//	Phase 2: Start (non-blocking)
//	  source.Scan() -> groups.Start()
//	  Launches producer and worker goroutines internally.
//
//	Phase 3: Async (errgroup, concurrent)
//	  ┌─ source.Finish()      - waits for scan, closes ReceiveChan
//	  ├─ transport()           - reads ReceiveChan, fans out to groups
//	  ├─ groups.Finish()       - waits for runners, closes OutChans
//	  └─ groups.OutputForward() - fan-in -> destination.Receive + Done
//	  All run concurrently. First error cancels asyncCtx (fail-fast).
//
//	Phase 4: Finish (synchronous, after errgroup)
//	  groups.DestinationFinish()
//	  Flushes destination buffers. Runs only if Phase 3 succeeded.
//
//	Phase 5: Close (deferred, always runs)
//	  source.Close() -> groups.Close()
//	  Releases resources. Collects all errors.
//
// # Context Hierarchy
//
// Two-layer context prevents destination flush races:
//
//	rootCtx (full lifecycle: Prepare -> Close)
//	├── asyncCtx (errgroup, Phase 3)
//	│   ├── source scanCtx      - fail-fast: cancel scan on error
//	│   ├── runner startCtx     - fail-fast: cancel workers on error
//	│   ├── transport            - fail-fast: stop forwarding on error
//	│   └── OutputForward        - fail-fast: stop receiving on error
//	└── destination writeCtx     - NOT canceled by asyncCtx; preserves flush ability
//
// Why two layers: When a worker fails, asyncCtx is canceled to stop
// remaining goroutines (fail-fast). But destination workers must still
// flush buffered data during Phase 4 (DestinationFinish). If destination
// writeCtx derived from asyncCtx, it would be canceled before the flush,
// and workers would exit without persisting buffered items.
//
// # Cascading Shutdown
//
// Phase 3 goroutines shut down in cascade through channel closures:
//
//	source.Finish closes ReceiveChan
//	  -> transport sees closed, calls groups.Done (closes runner inChan)
//	    -> runners.Finish waits for workers, closes OutChans
//	      -> fan-in drains, closes merged channel
//	        -> OutputForward sees closed, calls destination.Done
//
// # Multi-Stage Pipeline
//
// A multi-stage pipeline chains multiple Runners as stages, where each
// stage's output feeds the next stage's input. This is achieved by
// [exec.NewMultiStage] and [exec.Stage], which build an [exec.Runner]
// that internally chains sub-runners with type erasure:
//
//	Source -> Stage1(E1->E2) -> Stage2(E2->E3) -> ... -> Destination
//
// The multi-stage runner implements [exec.Runner] directly, so it can be
// passed to WithGroup like any simple runner. Branching (fan-out to
// multiple pipelines) is achieved by placing multiple runners in a group.
package flow
