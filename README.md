# Jack Service (Jobs At ClerK service)

Background job queue microservice that owns job enqueueing, with pluggable queue backends.

## Architecture

```
┌─────────────────┐     gRPC      ┌─────────────────┐
│   Applications  │──────────────▶│   Jack Service  │
└─────────────────┘               └────────┬────────┘
                                           │
                              ┌────────────┴────────────┐
                              │                         │
                              ▼                         ▼
                      ┌──────────────┐         ┌──────────────┐
                      │   Pub/Sub    │         │     Noop     │
                      │   Backend    │         │   Backend    │
                      └──────────────┘         └──────────────┘
```

### Components

- **gRPC API** - `Enqueue` and `EnqueueBulk` endpoints for job submission
- **Storage** - Producer and job type configuration (GCS or in-memory)
- **Queue Backends** - Pluggable job delivery (Pub/Sub or noop for testing)
- **Web Console** - HTTP UI for managing producers and job types

## API

### Enqueue

Submit a single job for background processing.

```protobuf
rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| producer_id | string | Yes | Registered producer identifier |
| job_type | string | Yes | Job type name |
| payload | bytes | No | Opaque data delivered to consumer |
| run_at | Timestamp | No | Scheduled execution time (immediate if unset) |
| trace_id | string | No | Distributed tracing correlation ID |

### EnqueueBulk

Submit multiple jobs in a single request (best-effort, non-atomic).

```protobuf
rpc EnqueueBulk(EnqueueBulkRequest) returns (EnqueueBulkResponse);
```

### Priority Queues

Jobs are routed to priority queues based on job type configuration:

| Queue | SLA |
|-------|-----|
| Immediate | ≤5 seconds |
| High | ≤15 seconds |
| Medium | ≤1 minute |
| Low | ≤5 minutes |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | 50051 | gRPC server port |
| `HTTP_PORT` | 8080 | HTTP server port (health checks, web console) |
| `GCS_BUCKET` | (none) | GCS bucket for configuration storage |
| `GCS_PREFIX` | (none) | Object prefix within bucket |
| `QUEUE_BACKEND` | noop | Queue backend: `noop`, `pubsub` |
| `PUBSUB_PROJECT` | (none) | GCP project for Pub/Sub |

## Prerequisites

- Go 1.25+
- [buf](https://buf.build/docs/installation) (`make install-tools`)

## Development

```bash
# Install tools (buf CLI)
make install-tools

# Generate protobuf code
make generate

# Build
make build

# Run locally (uses in-memory storage and noop queue)
make run

# Run tests
make test
```

### Endpoints

| URL | Description |
|-----|-------------|
| `localhost:50051` | gRPC API |
| `http://localhost:8080/health` | Health check |
| `http://localhost:8080/` | Web console - manage producers and job types |
| `http://localhost:8080/grpc/` | gRPC UI - test API calls in the browser |

## Protobuf

This project uses [buf](https://buf.build/) for protobuf management.

### Generating Code

```bash
make generate
```

Generates Go code from `.proto` files into `proto/jackpb/`.

### Breaking Change Detection

```bash
make breaking
```

Compares current proto definitions against the `main` branch and fails if there are backwards-incompatible changes. This catches:

- Renamed fields
- Deleted fields
- Changed field numbers
- Changed field types

Run this before merging proto changes to ensure API compatibility.

### Linting

```bash
make lint
```

Checks proto files against buf's standard lint rules.

## Deployment

```bash
# Build Docker image
docker build -t jack-service .

# Run with GCS storage
docker run -e GCS_BUCKET=my-bucket -e QUEUE_BACKEND=pubsub -e PUBSUB_PROJECT=my-project jack-service
```
