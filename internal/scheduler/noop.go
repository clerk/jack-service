package scheduler

import (
	"context"
	"log"

	"github.com/clerk/jack-service/internal/queue"
)

// NoopScheduler is a no-op scheduler for development and testing.
// It logs scheduled jobs but does not create Cloud Tasks.
type NoopScheduler struct{}

// NewNoopScheduler creates a new no-op scheduler.
func NewNoopScheduler() *NoopScheduler {
	return &NoopScheduler{}
}

// Schedule logs the job and returns success.
func (s *NoopScheduler) Schedule(_ context.Context, job *queue.Job) error {
	log.Printf("[noop-scheduler] Would schedule job %s (type=%s, run_at=%s)", job.ID, job.Type, job.RunAt)
	return nil
}

// ScheduleBulk logs and returns success for all jobs.
func (s *NoopScheduler) ScheduleBulk(_ context.Context, jobs []*queue.Job) []queue.EnqueueResult {
	results := make([]queue.EnqueueResult, len(jobs))
	for i, job := range jobs {
		log.Printf("[noop-scheduler] Would schedule job %s (type=%s, run_at=%s)", job.ID, job.Type, job.RunAt)
		results[i] = queue.EnqueueResult{JobID: job.ID}
	}
	return results
}

// Close is a no-op.
func (s *NoopScheduler) Close() error { return nil }

// Ensure NoopScheduler implements Scheduler.
var _ Scheduler = (*NoopScheduler)(nil)
