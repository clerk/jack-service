package api

import (
	"context"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/clerk/jack-service/internal/queue"
	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/proto/jackpb"
)

type captureBackend struct {
	jobs []*queue.Job
}

func (b *captureBackend) Enqueue(_ context.Context, job *queue.Job) error {
	b.jobs = append(b.jobs, job)
	return nil
}

func (b *captureBackend) EnqueueBulk(_ context.Context, jobs []*queue.Job) []queue.EnqueueResult {
	b.jobs = append(b.jobs, jobs...)
	results := make([]queue.EnqueueResult, len(jobs))
	for i, job := range jobs {
		results[i] = queue.EnqueueResult{JobID: job.ID}
	}
	return results
}

func (b *captureBackend) Health(context.Context) error { return nil }
func (b *captureBackend) Close() error                 { return nil }

func TestEnqueueBulkUsesLegacyPayloadQueueWhenEnabled(t *testing.T) {
	server, backend := newRoutingTestServer(t, true)

	enqueueRoutingTestJob(t, server, []byte(`{"queue":"high","args":{"InstanceID":"ins_123"}}`))

	if got := capturedPriority(t, backend); got != queue.PriorityHigh {
		t.Fatalf("priority = %s, want high", got)
	}
}

func TestEnqueueBulkSkipsJobTypeRegistryWhenLegacyPayloadQueueExists(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStore()
	if err := store.CreateProducer(ctx, &jackpb.Producer{ProducerId: "client-api", Name: "client-api"}); err != nil {
		t.Fatalf("CreateProducer: %v", err)
	}

	backend := &captureBackend{}
	config := DefaultServerConfig()
	config.LegacyPayloadQueueRouting = true
	server := NewServer(store, backend, nil, config, &statsd.NoOpClient{})

	resp, err := server.EnqueueBulk(ctx, &jackpb.EnqueueBulkRequest{Jobs: []*jackpb.EnqueueRequest{
		{
			ProducerId: "client-api",
			JobType:    "SendEmail",
			Payload:    []byte(`{"queue":"high","args":{"InstanceID":"ins_123"}}`),
		},
	}})
	if err != nil {
		t.Fatalf("EnqueueBulk: %v", err)
	}
	if got := capturedPriority(t, backend); got != queue.PriorityHigh {
		t.Fatalf("priority = %s, want high", got)
	}
	if got := resp.Results[0].ErrorMessages; len(got) != 0 {
		t.Fatalf("warnings = %v, want none", got)
	}
}

func TestEnqueueUsesLegacyPayloadQueueWhenEnabled(t *testing.T) {
	server, backend := newRoutingTestServer(t, true)

	_, err := server.Enqueue(context.Background(), &jackpb.EnqueueRequest{
		ProducerId: "client-api",
		JobType:    "SendEmail",
		Payload:    []byte(`{"queue":"high","args":{"InstanceID":"ins_123"}}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := capturedPriority(t, backend); got != queue.PriorityHigh {
		t.Fatalf("priority = %s, want high", got)
	}
}

func TestEnqueueBulkUsesRegistryQueueWhenLegacyPayloadQueueDisabled(t *testing.T) {
	server, backend := newRoutingTestServer(t, false)

	enqueueRoutingTestJob(t, server, []byte(`{"queue":"high","args":{"InstanceID":"ins_123"}}`))

	if got := capturedPriority(t, backend); got != queue.PriorityMedium {
		t.Fatalf("priority = %s, want medium", got)
	}
}

func TestEnqueueBulkFallsBackToRegistryQueueForInvalidLegacyPayloadQueue(t *testing.T) {
	server, backend := newRoutingTestServer(t, true)

	enqueueRoutingTestJob(t, server, []byte(`{"queue":"urgent","args":{"InstanceID":"ins_123"}}`))

	if got := capturedPriority(t, backend); got != queue.PriorityMedium {
		t.Fatalf("priority = %s, want medium", got)
	}
}

func TestEnqueueBulkFallsBackToRegistryQueueWhenLegacyPayloadQueueMissing(t *testing.T) {
	server, backend := newRoutingTestServer(t, true)

	enqueueRoutingTestJob(t, server, []byte(`{"args":{"InstanceID":"ins_123"}}`))

	if got := capturedPriority(t, backend); got != queue.PriorityMedium {
		t.Fatalf("priority = %s, want medium", got)
	}
}

func newRoutingTestServer(t *testing.T, legacyPayloadQueueRouting bool) (*Server, *captureBackend) {
	t.Helper()

	ctx := context.Background()
	store := storage.NewMemoryStore()
	if err := store.CreateProducer(ctx, &jackpb.Producer{ProducerId: "client-api", Name: "client-api"}); err != nil {
		t.Fatalf("CreateProducer: %v", err)
	}
	if err := store.CreateJobType(ctx, &jackpb.JobType{
		ProducerId: "client-api",
		Name:       "SendEmail",
		Queue:      jackpb.Queue_QUEUE_MEDIUM,
	}); err != nil {
		t.Fatalf("CreateJobType: %v", err)
	}

	backend := &captureBackend{}
	config := DefaultServerConfig()
	config.LegacyPayloadQueueRouting = legacyPayloadQueueRouting
	server := NewServer(store, backend, nil, config, &statsd.NoOpClient{})
	return server, backend
}

func enqueueRoutingTestJob(t *testing.T, server *Server, payload []byte) {
	t.Helper()

	_, err := server.EnqueueBulk(context.Background(), &jackpb.EnqueueBulkRequest{Jobs: []*jackpb.EnqueueRequest{
		{
			ProducerId: "client-api",
			JobType:    "SendEmail",
			Payload:    payload,
		},
	}})
	if err != nil {
		t.Fatalf("EnqueueBulk: %v", err)
	}
}

func capturedPriority(t *testing.T, backend *captureBackend) queue.Priority {
	t.Helper()

	if len(backend.jobs) != 1 {
		t.Fatalf("captured jobs = %d, want 1", len(backend.jobs))
	}
	return backend.jobs[0].Priority
}
