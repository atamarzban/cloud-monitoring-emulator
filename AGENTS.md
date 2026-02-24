# AGENTS.md

Local emulator for the GCP Cloud Monitoring API v3. Serves gRPC, REST/JSON, Prometheus query, and admin APIs on a single port via cmux connection multiplexing.

## Build / Test / Run

```bash
make build       # → ./bin/emulator
make test        # go test -race -count=1 ./...
make run         # build + run on :8080
make generate    # regenerate proto code (requires buf CLI)
make docker      # docker build
```

Run with a custom port: `./bin/emulator -port 9090`

## Project Structure

```
cmd/emulator/main.go             Entry point — cmux wiring, graceful shutdown
internal/
  server/metric_service.go       gRPC MetricServiceServer (9 methods)
  server/alert_policy_service.go gRPC AlertPolicyServiceServer (5 methods)
  store/store.go                 Store interface (all persistence behind this)
  store/memory.go                In-memory Store implementation (sync.RWMutex)
  filter/                        Hand-written lexer/parser/evaluator for GCP monitoring filter language
  validation/                    Request validation helpers
  promql/adapter.go              Bridges Store → Prometheus storage.Queryable
  promql/handler.go              Prometheus HTTP API handlers
  admin/                         Admin API (POST /admin/reset, GET /admin/state)
  integration/                   Full-stack integration tests
gen/google/monitoring/v3/        Generated protobuf/gRPC/gateway code (do not edit)
proto/                           buf generation config
```

## Architecture

- **Single port, multiplexed**: cmux dispatches gRPC (HTTP/2 `content-type: application/grpc`) to the gRPC server; everything else goes to an HTTP mux serving REST, Prometheus, and admin routes.
- **In-process grpc-gateway**: REST endpoints are transcoded in-process via `HandlerServer` (no loopback dial).
- **Store interface** (`internal/store/store.go`): All services receive a `store.Store` via constructor injection. The only implementation is `MemoryStore`.
- **Thread safety**: `MemoryStore` uses `sync.RWMutex`. All proto messages are deep-cloned via `proto.Clone` on read and write.
- **Series identity**: Built from metric type + sorted labels + resource type + sorted resource labels.
- **Auto-create descriptors**: `CreateTimeSeries` infers and creates metric descriptors that don't exist yet.
- **Prometheus name mapping**: Domain dots → underscores, first slash → colon, path slashes → underscores (e.g. `custom.googleapis.com/my_metric` → `custom_googleapis_com:my_metric`).

## Testing

- Unit tests live alongside source files (`*_test.go` in each package).
- Integration tests in `internal/integration/integration_test.go` spin up the full stack on a random port (`127.0.0.1:0`) and exercise gRPC, REST, PromQL, admin, concurrency, pagination, filtering, and validation.
- All tests run with `-race`.

## Conventions

- Go 1.25, modules enabled.
- Structured logging via `log/slog`.
- Pagination uses base64-encoded offset tokens.
- Validation returns proper gRPC status codes (`InvalidArgument`, `NotFound`, `AlreadyExists`).
- Generated code lives in `gen/` — regenerate with `make generate`, do not edit by hand.
