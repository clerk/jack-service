// Package scheduler provides delayed job scheduling via GCP Cloud Tasks.
//
// When a job's RunAt is sufficiently far in the future, the scheduler creates
// a Cloud Task that fires at that time. The task calls back to Jack's HTTP
// endpoint, which publishes the job to Pub/Sub immediately.
package scheduler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/clerk/jack-service/internal/queue"
)

// Scheduler defines the interface for delayed job scheduling.
type Scheduler interface {
	// Schedule creates a delayed task that will fire at job.RunAt.
	Schedule(ctx context.Context, job *queue.Job) error

	// ScheduleBulk handles multiple jobs. Returns per-job results.
	ScheduleBulk(ctx context.Context, jobs []*queue.Job) []queue.EnqueueResult

	// Close cleans up resources.
	Close() error
}

// scheduledJob is the JSON representation of a queue.Job for Cloud Tasks payloads.
type scheduledJob struct {
	ID         string `json:"id"`
	ProducerID string `json:"producer_id"`
	Type       string `json:"type"`
	Priority   int    `json:"priority"`
	Payload    []byte `json:"payload"`
	RunAt      string `json:"run_at"`
	TraceID    string `json:"trace_id"`
	MaxRetries int    `json:"max_retries"`
	CreatedAt  string `json:"created_at"`
}

// marshalJob serializes a queue.Job to JSON for the Cloud Tasks body.
func marshalJob(job *queue.Job) ([]byte, error) {
	sj := scheduledJob{
		ID:         job.ID,
		ProducerID: job.ProducerID,
		Type:       job.Type,
		Priority:   int(job.Priority),
		Payload:    job.Payload,
		RunAt:      job.RunAt.UTC().Format(time.RFC3339Nano),
		TraceID:    job.TraceID,
		MaxRetries: job.MaxRetries,
		CreatedAt:  job.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	return json.Marshal(sj)
}

// UnmarshalJob deserializes a JSON payload back into a queue.Job.
func UnmarshalJob(data []byte) (*queue.Job, error) {
	var sj scheduledJob
	if err := json.Unmarshal(data, &sj); err != nil {
		return nil, err
	}

	runAt, err := time.Parse(time.RFC3339Nano, sj.RunAt)
	if err != nil {
		return nil, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, sj.CreatedAt)
	if err != nil {
		return nil, err
	}

	return &queue.Job{
		ID:         sj.ID,
		ProducerID: sj.ProducerID,
		Type:       sj.Type,
		Priority:   queue.Priority(sj.Priority),
		Payload:    sj.Payload,
		RunAt:      runAt,
		TraceID:    sj.TraceID,
		MaxRetries: sj.MaxRetries,
		CreatedAt:  createdAt,
	}, nil
}
