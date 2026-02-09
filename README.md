# Jack Service (Jobs At ClerK service)

Background job queue microservice that owns job enqueueing, with pluggable queue backends.

## Architecture

```
┌─────────────────┐     gRPC      ┌─────────────────┐
│   Applications  │──────────────▶│   Jack Service  │
└─────────────────┘               └────────┬────────┘
                                           │
┌─────────────────┐     gRPC      ┌────────┴────────┐
│   Jack Console  │──────────────▶│  AdminService   │
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
- **AdminService** - gRPC API for managing producers and job types (used by [jack-console](https://github.com/clerk/jack-console))
- **Storage** - Producer and job type configuration (GCS or in-memory)
- **Queue Backends** - Pluggable job delivery (Pub/Sub or noop for testing)
- **Health** - Standard gRPC Health Checking Protocol

## API

### BackgroundJobs Service

#### Enqueue

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

#### EnqueueBulk

Submit multiple jobs in a single request (best-effort, non-atomic).

```protobuf
rpc EnqueueBulk(EnqueueBulkRequest) returns (EnqueueBulkResponse);
```

### AdminService

CRUD operations for producers and job types.

```protobuf
rpc CreateProducer(CreateProducerRequest) returns (CreateProducerResponse);
rpc GetProducer(GetProducerRequest) returns (GetProducerResponse);
rpc ListProducers(ListProducersRequest) returns (ListProducersResponse);
rpc CreateJobType(CreateJobTypeRequest) returns (CreateJobTypeResponse);
rpc ListJobTypes(ListJobTypesRequest) returns (ListJobTypesResponse);
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

### Verifying with grpcurl

```bash
# List all services
grpcurl -plaintext localhost:50051 list

# Health check
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# List producers
grpcurl -plaintext localhost:50051 jack.AdminService/ListProducers

# List job types for a producer
grpcurl -plaintext -d '{"producer_id": "prod_XXXXXXXX"}' localhost:50051 jack.AdminService/ListJobTypes

# Enqueue a job
grpcurl -plaintext -d '{"producer_id": "prod_XXXXXXXX", "job_type": "send_email"}' localhost:50051 jack.BackgroundJobs/Enqueue
```

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
