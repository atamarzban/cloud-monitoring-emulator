# Cloud Monitoring Emulator

A local emulator for the [GCP Cloud Monitoring API v3](https://cloud.google.com/monitoring/api/ref_v3/rest). Serves gRPC, REST, and a Prometheus-compatible query API on a single port.

## Quick Start

```bash
# Build
make build

# Run (default port 8080)
./bin/emulator

# Run on a custom port
./bin/emulator -port 9090
```

## API Surfaces

All four APIs are served on a single port via connection multiplexing (cmux):

| API | Protocol | Base Path |
|-----|----------|-----------|
| Cloud Monitoring gRPC | gRPC | (same port) |
| Cloud Monitoring REST | HTTP/JSON | `/v3/projects/{project}/...` |
| Prometheus Query | HTTP/JSON | `/v1/projects/{project}/location/{location}/prometheus/api/v1/` |
| Admin | HTTP/JSON | `/admin/` |

### Cloud Monitoring API

Implements the following services:

**MetricService** (9 methods):
- `CreateMetricDescriptor` / `GetMetricDescriptor` / `ListMetricDescriptors` / `DeleteMetricDescriptor`
- `GetMonitoredResourceDescriptor` / `ListMonitoredResourceDescriptors`
- `CreateTimeSeries` / `CreateServiceTimeSeries` / `ListTimeSeries`

**AlertPolicyService** (5 methods):
- `CreateAlertPolicy` / `GetAlertPolicy` / `ListAlertPolicies` / `UpdateAlertPolicy` / `DeleteAlertPolicy`

Works with standard GCP client libraries. Point them at the emulator by setting the endpoint:

```go
import monitoring "cloud.google.com/go/monitoring/apiv3/v2"

client, _ := monitoring.NewMetricClient(ctx,
    option.WithEndpoint("localhost:8080"),
    option.WithoutAuthentication(),
    option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
)
```

### Prometheus Query API

Supports PromQL queries over ingested time series:

```bash
# Instant query
curl 'http://localhost:8080/v1/projects/my-project/location/global/prometheus/api/v1/query?query=custom_googleapis_com:my_metric'

# Range query
curl 'http://localhost:8080/v1/projects/my-project/location/global/prometheus/api/v1/query_range?query=custom_googleapis_com:my_metric&start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&step=60'

# Series metadata
curl 'http://localhost:8080/v1/projects/my-project/location/global/prometheus/api/v1/series?match[]=custom_googleapis_com:my_metric'

# Label names and values
curl 'http://localhost:8080/v1/projects/my-project/location/global/prometheus/api/v1/labels'
curl 'http://localhost:8080/v1/projects/my-project/location/global/prometheus/api/v1/label/__name__/values'
```

GCP metric types are mapped to Prometheus names: domain dots become underscores, the first slash becomes a colon, path slashes become underscores. For example:
- `custom.googleapis.com/my_metric` becomes `custom_googleapis_com:my_metric`
- `compute.googleapis.com/instance/cpu/utilization` becomes `compute_googleapis_com:instance_cpu_utilization`

### Admin API

```bash
# Reset all state (clears all projects, metrics, time series, alert policies)
curl -X POST http://localhost:8080/admin/reset

# Get summary of current state
curl http://localhost:8080/admin/state
```

## Monitoring Filter Language

`ListTimeSeries` and `ListMetricDescriptors` support the GCP [monitoring filter language](https://cloud.google.com/monitoring/api/v3/filters):

```
metric.type = "custom.googleapis.com/my_metric"
metric.type = "custom.googleapis.com/my_metric" AND metric.labels.env = "prod"
resource.type = "gce_instance" OR resource.type = "k8s_container"
NOT resource.labels.zone = "us-east1-b"
metric.type = starts_with("custom.googleapis.com/")
metric.labels.env = one_of("prod", "staging")
metric.type : monitoring.regex.full_match("custom\\.googleapis\\.com/.*")
```

Supported operators: `=`, `!=`, `:` (has/substring), `AND`, `OR`, `NOT`

Supported functions: `starts_with()`, `ends_with()`, `one_of()`, `monitoring.regex.full_match()`

## Seeded Resource Types

The emulator comes pre-loaded with common `MonitoredResourceDescriptor` types:
`global`, `gce_instance`, `k8s_container`, `k8s_pod`, `k8s_node`, `k8s_cluster`, `aws_ec2_instance`, `gae_app`

## Validation

The emulator enforces the same validation rules as the real API:
- Max 200 time series per `CreateTimeSeries` request
- Exactly 1 point per time series entry
- Custom metric types must start with `custom.googleapis.com/` or `external.googleapis.com/`
- Required fields (metric, resource, interval, value) are checked
- Proper gRPC status codes (InvalidArgument, NotFound, AlreadyExists)

## Development

```bash
make build     # Build the binary
make test      # Run all tests with race detector
make run       # Build and run
make generate  # Regenerate proto code (requires buf)
make clean     # Remove build artifacts
```

## Project Structure

```
cmd/emulator/main.go           - Entry point, cmux wiring
internal/
  server/                      - gRPC service handlers (MetricService, AlertPolicyService)
  store/store.go               - Storage interface
  store/memory.go              - In-memory implementation
  filter/                      - Monitoring filter language (lexer, parser, evaluator)
  validation/                  - Request validation helpers
  promql/adapter.go            - Prometheus storage.Queryable adapter
  promql/handler.go            - Prometheus HTTP API handlers
  admin/                       - Admin API (reset, state)
  integration/                 - Full-stack integration tests
gen/google/monitoring/v3/      - Generated proto code
proto/                         - buf configuration
```
