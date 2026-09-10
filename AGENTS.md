# AGENTS.md — core

The SpeechMux Core server (Go). Assumes the workspace root `AGENTS.md`; only what is
specific to this repository is here.

Root instructions: `../AGENTS.md` · Pipeline internals:
`../docs/architecture/core-pipeline.md` · Package map:
`../docs/architecture/overview.md#core-packages`

---

## Role

One binary, `speechmux-core`, with two modes:

| Invocation | What runs |
|------------|-----------|
| `speechmux-core --config core.yaml --plugins plugins.yaml` | The server: gRPC `:50051`, HTTP `:8090`, WebSocket `:8091` |
| `speechmux-core ctl start\|status\|stop --workspace workspace.yaml` | The process supervisor for VAD, STT and Core (`internal/ctl`) |

`ctl` is dispatched on `os.Args[1] == "ctl"` **before** `flag.Parse()` in
`cmd/speechmux-core/main.go`, with its own `FlagSet`. Keep it that way; do not add a CLI
framework for three subcommands (`../docs/decisions/0010-ctl-subcommand-process-manager.md`).

Core owns everything stateful: sessions, buffering, EPD, routing, backpressure, failure
recovery. Plugins only run models. If you find yourself adding session logic to a plugin or
model logic to Core, stop.

## Where things live

| Package | Owns | Touch it when |
|---------|------|---------------|
| `internal/runtime` | `Application`: wiring, `Run`, 3-phase shutdown, SIGHUP reload, `/health` checker | Adding a subsystem, changing startup/shutdown order |
| `internal/transport` | gRPC server, WebSocket handler, HTTP + admin routes | Wire-format or route changes — **update `../docs/api/client-protocol.md`** |
| `internal/session` | `Session`, `Manager`, auth, park/resume, idle reaper | Session lifecycle, limits, auth profiles |
| `internal/stream` | The pipeline: processor, ring buffer, aggregator, EPD, both `DecodeEngine`s, fair dispatcher, result assembler | Anything between audio-in and result-out |
| `internal/plugin` | `Endpoint` + circuit breaker, VAD/inference clients, `PluginRouter` | Talking to plugins, routing, health probes |
| `internal/config` | YAML structs, `Defaults()`, `Validate()`, TLS | New config key — use the `add-config-option` skill |
| `internal/errors` | `ERR####` registry, gRPC/HTTP mapping | New code — use the `add-error-code` skill |
| `internal/codec` | Encoding → PCM S16LE at target rate | New audio encoding |
| `internal/metrics`, `internal/tracing`, `internal/health` | Prometheus, OTel, health types | New signal — see `../docs/architecture/observability.md` |
| `internal/storage` | Optional async audio recording | Recording behaviour |
| `internal/ratelimit` | Token buckets | Rate-limit semantics |
| `internal/ctl` | Supervisor, workspace.yaml profiles | Process management |
| `tools/loadtest` | Client-side load driver | Benchmarking |

`internal/health` exists only to break an import cycle between `transport` and `runtime`.
Do not put logic in it.

## The proto dependency

`go.mod` has a **committed** `replace github.com/speechmux/proto => ../proto`. Core compiles
against the sibling `proto/` checkout, not a published module. Consequences:

- `proto/` must be cloned beside `core/` or Core does not build.
- A regenerated `proto/gen/go` is visible here immediately.
- Nothing pins the two repos to compatible commits. Commit proto first and mention the
  proto commit in the Core commit message. Follow the `change-proto` skill.

## Build, test, lint

```bash
make build        # go build -o bin/speechmux-core ./cmd/speechmux-core
make test         # go test -race ./...          (root `make test` runs without -race)
make run          # build + run with config/core.yaml + config/plugins.yaml
make loadtest     # builds bin/loadtest; its help text still mentions a removed `run-dummy` target
go vet ./...      # clean
make lint         # golangci-lint — NOT installed locally; use go vet until it is
gofmt -l .        # 29 pre-existing files (hand-aligned comments); do not mass-reformat
```

Run `-race` for any change under `stream`, `session` or `transport`. `internal/stream` and
`internal/transport` are the slow packages (~10 s each); run them alone while iterating:
`go test ./internal/stream/ -run TestFair -v`.

`internal/tracing`, `internal/metrics` and `internal/health` have no tests
(`../docs/plans/test-and-lint-gaps.md`).

## Invariants that are easy to break

These are the things a reasonable change breaks without noticing. Each one has cost a bug.

**Session pipeline (`stream/processor.go`)**

- The `defer` in `ProcessSession` runs `engine.Close()` → `SignalPipelineExit(err)` →
  `close(ResultCh)` → `MarkProcessingDone()`, **in that order**. The transport must see the
  error before the result channel closes, or it sends a clean `done` for a failed session.
- The trim ticker runs **outside** the errgroup on purpose. Inside it, a trim error would
  kill the session.
- `PinByHint()` is one call that both routes and pins. Never split it back into
  `Route()` + `PinByHint()` — in a mixed pool the two could pick different endpoints.
- The batch path unpins immediately; the streaming path holds the pin for the session.
- `VADClient.Close()` half-closes send and must **not** cancel the stream context, or the
  last utterance's VAD results are lost.
- `SessionProcessor` is assigned through an interface-typed variable so that a nil concrete
  `*StreamProcessor` does not become a non-nil interface. Do not "simplify" that.

**Engines (`stream/engine.go`, `batch_engine.go`, `streaming_engine.go`)**

- `DecodeEngine.Close()` is idempotent and must return only after `Results()` is closed.
- Adding a field to `DecodeResult` **requires** a matching line in the result-forwarding
  goroutine in `ProcessSession`, or it is silently dropped on the wire.
- An engine snapshots `config.StreamConfig` at `Start`. A hot reload does not reach a
  running engine. If a new key must be live, do not read it from the snapshot.
- `streamingDecodeEngine` stores `terminalErr` **before** cancelling, and `Close()` drains
  `recvLoop` before `cancel()`. `recvLoop` alone closes `resultsCh`.
- `batchFrameMs = 30` in `batch_engine.go` must equal `optFrameMs` in `processor.go` —
  it is the sequence-number → seconds factor for every timestamp
  (`../docs/plans/vad-frame-size-negotiation.md`).

**Fair dispatcher (`stream/fair_dispatch.go`)**

Six invariants are written up in
`../docs/architecture/core-pipeline.md#fairdecodedispatcher`. Read them before editing.
The short version: `releaseInFlight` is called explicitly, never deferred, and calls
`notify()` **last**; a slot with an empty queue but `inFlight=true` stays in `d.sessions`;
every `resultCh` gets exactly one value and is never closed; `CancelSession` runs before
`decodeQueueCh` is closed; `collectorDone` before `close(resultsCh)`. Getting any of these
wrong deadlocks rather than fails.

**Routing (`plugin/router.go`)**

- `RouteBatch()` filters on `== STREAMING_MODE_BATCH_ONLY`, not `!= NATIVE`. An endpoint
  whose capabilities are still `UNSPECIFIED` is excluded until the probe fills them in.
- Capabilities are fetched at `Add()`, non-fatally, with a background retry and a re-fetch
  in `probeAll()` while still `UNSPECIFIED`. Do not make the fetch fatal — plugins load
  slowly and Core routinely starts first.
- `FetchCapabilities` runs **before** taking the router's write lock.

**Runtime (`runtime/application.go`)**

- `StartHealthProbe` uses `gCtx`, not `context.Background()`, so it stops at shutdown.
- `reloadConfig` is serialised by `reloadMu`; readers go through `atomic.Pointer[Config]`.
- TLS config is built once in `New()`. Reload does not re-read certs
  (`../docs/decisions/0009-shared-tls-config-restart-to-rotate.md`).
- Shutdown order: `StopAccepting` → `GracefulStop` → `DrainAll` → `Stop` →
  `streamProc.Close()` → flush traces. The dispatcher must shut down **after** sessions drain.

**Errors (`errors/codes.go`)**

- Wrap with `%w`, never `%v`, or `errors.As` misses the `*STTError` and the client sees
  ERR3002.
- `retryable` is derived from the gRPC code. Pick the code with that in mind.
- `contract_test.go` fails when a registered code has no entry. That is intended.

**Metrics**

- `session` and `stream` take `metrics.MetricsObserver`; they never import Prometheus.
  Tests pass `metrics.NopMetrics{}`.
- `streaming_session_terminations_total.reason` is a **closed enum**. Add a named reason;
  never an `"other"` bucket.

## Configuration

`config/core.yaml` and `config/plugins.yaml` are the reference configs; `config/plugins-dummy.yaml`
pairs with the dummy plugins for load testing. `config/workspace.yaml` is a self-contained
example — the supervisor actually used by `make up` is the **workspace root** `workspace.yaml`.

Every `core.yaml` key must also land in `../deploy/docker/core-docker.yaml`; the two have
already drifted (`../docs/operations/configuration.md#known-config-drift`). Code defaults
in `Defaults()` differ from the file values in places (ports 8000/8001 vs 8090/8091,
`max_sessions` 50 vs 1000) — the file wins when the key is present.

## Testing conventions

- Table-driven, `t.Run` subtests, `t.Cleanup`. Helpers: `session.NewTestSession` in
  `session/testutil.go`; `session/export_test.go` exposes internals to the package's tests.
- **Plugins are stubbed in Go.** Integration tests implement `InferencePluginServer` /
  `VADPluginServer` in-process over `bufconn` (`transport/pipeline_integration_test.go`,
  `stream/*_integration_test.go`). Never start a Python process from a Go test.
- The `bufconn` dialer passed to `grpc.WithContextDialer` must return a concrete `net.Conn`,
  not an anonymous interface, or the dial fails opaquely.
- Batch-path tests **drain** results before asserting: in-flight partials arrive before the
  final.
- Streaming integration tests need ~50 PCM frames before the EPD declares utterance end at
  the default `vad_silence_sec`.
- `internal/stream` integration tests live in package `stream_test` and share helpers
  across files; put new shared helpers there, not in a new `_test.go` with its own copies.

## Logging

`slog` everywhere. Per-frame EPD/VAD logging is DEBUG only; INFO liveness comes from the
`epd_heartbeat_interval_sec` heartbeat and streak-debounced transitions. Lag warnings in
REALTIME mode are rate-limited to one per 30 s. Any new hot-path log line must be DEBUG or
rate-limited.

## Do not

- Put model or engine-specific logic in Core. It belongs in a plugin.
- Hand-edit `../proto/gen/go/**` or `bin/**`.
- Reorder the `ProcessSession` defer chain or the shutdown phases.
- Import `prometheus` from `session` or `stream`.
- Run `gofmt -w` across the tree as part of a feature change.
- Add a `cobra`-style CLI for `ctl`.
- Read config anywhere except through `cfg.Load()` (or the engine's `Start` snapshot,
  knowingly).
- Change an existing `ERR####` row or an accepted ADR's decision.

## Known gaps in this repo

- `batch_engine.go:353` sends `DecodeOptions: nil` and hardcodes `TASK_TRANSCRIBE` — the
  client's `decode_profile` and `task` never reach the plugin
  (`../docs/plans/decode-options-and-task-passthrough.md`).
- `optFrameMs = 30` is hardcoded; `VADCapabilities.optimal_frame_ms` is never read
  (`../docs/plans/vad-frame-size-negotiation.md`).
- `golangci-lint` not installed; `gofmt -l` baseline of 29 files
  (`../docs/plans/test-and-lint-gaps.md`).

## Related

- Skills: `../.codex/skills/change-proto/`, `add-config-option/`, `add-error-code/`,
  `verify-workspace/`
- ADRs most relevant here: 0002, 0005, 0006, 0007, 0008, 0009, 0010, 0011, 0013, 0014 in
  `../docs/decisions/`
