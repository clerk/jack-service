package queue

import (
	"context"
	"log"
)

// NoopBackend is a no-operation queue backend for testing and development.
// It accepts all jobs but doesn't actually queue them anywhere.
type NoopBackend struct {
	config BackendConfig
}

// NewNoopBackend creates a new no-op queue backend.
func NewNoopBackend(config BackendConfig) *NoopBackend {
	return &NoopBackend{config: config}
}

// Enqueue logs the job but doesn't actually queue it.
func (n *NoopBackend) Enqueue(ctx context.Context, job *Job) error {
	if err := job.Validate(n.config.MaxPayloadSize); err != nil {
		return err
	}

	log.Printf("[noop] enqueue job: id=%s producer=%s type=%s priority=%s payload_size=%d",
		job.ID, job.ProducerID, job.Type, job.Priority, len(job.Payload))

	return nil
}

// EnqueueBulk logs all jobs but doesn't actually queue them.
func (n *NoopBackend) EnqueueBulk(ctx context.Context, jobs []*Job) []EnqueueResult {
	results := make([]EnqueueResult, len(jobs))

	for i, job := range jobs {
		err := n.Enqueue(ctx, job)
		results[i] = EnqueueResult{
			JobID: job.ID,
			Error: err,
		}
	}

	return results
}

// Health always returns nil (healthy).
func (n *NoopBackend) Health(ctx context.Context) error {
	return nil
}

// Close is a no-op.
func (n *NoopBackend) Close() error {
	return nil
}

// Ensure NoopBackend implements Backend.
var _ Backend = (*NoopBackend)(nil)
