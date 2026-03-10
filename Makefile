.PHONY: generate build run test test-integration clean lint breaking

BUF := $(shell go env GOPATH)/bin/buf

# Generate protobuf code using buf
generate:
	$(BUF) generate

# Check for breaking changes against main branch
# This will fail if proto changes are not backwards compatible
breaking:
	@echo "Checking for breaking protobuf changes against main..."
	$(BUF) breaking --against '.git#branch=main'

# Check for breaking changes against HEAD (useful before committing)
breaking-head:
	@echo "Checking for breaking protobuf changes against HEAD..."
	$(BUF) breaking --against '.git#branch=HEAD~1'

# Lint protobuf files
lint:
	$(BUF) lint

# Build the service
build:
	go build -o bin/jack ./cmd/jack

# Run the service
run:
	go run ./cmd/jack

# Run tests
test:
	go test -v ./...

# Run integration tests (requires Pub/Sub emulator on localhost:8085)
test-integration:
	PUBSUB_EMULATOR_HOST=localhost:8085 go test -tags integration -v -timeout 120s ./internal/integration/

# Clean build artifacts
clean:
	rm -rf bin/

# Install tools (run once)
install-tools:
	go install github.com/bufbuild/buf/cmd/buf@latest

# Download dependencies
deps:
	go mod tidy
	go mod download
