FROM golang:1.25.0-bookworm AS builder

WORKDIR /app

# Install buf for proto generation
RUN go install github.com/bufbuild/buf/cmd/buf@latest

# Download dependencies first (cache layer)
COPY go.mod go.sum ./
COPY proto/jackpb/go.mod proto/jackpb/go.sum proto/jackpb/
RUN go mod download

# Copy source
COPY . ./

# Generate proto files and build
RUN buf generate && go build -mod=readonly -o jack ./cmd/jack

# Runtime image
FROM debian:bookworm-slim

RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy entrypoint script
COPY bin/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Copy binary
COPY --from=builder /app/jack /app/jack

# Copy Datadog serverless-init for tracing
COPY --from=gcr.io/datadoghq/serverless-init:1.8.2 /datadog-init /app/datadog-init

EXPOSE 50051

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/app/datadog-init", "/app/jack"]
