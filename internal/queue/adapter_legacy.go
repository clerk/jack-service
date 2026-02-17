package queue

import (
	"encoding/json"
	"time"
)

// LegacyAdapter produces Pub/Sub messages in the format expected by
// the legacy pubsubworker. The wire format is a JSON-serialized struct
// matching the legacy sqbmodel.PubsubJob shape.
type LegacyAdapter struct{}

// legacyPubsubJob matches the JSON tags of the legacy sqbmodel.PubsubJob exactly.
// Fields that the legacy worker reads: ID, JobType, Args, Queue, RunAt,
// LastError, ErrorCount.
type legacyPubsubJob struct {
	ID          string          `json:"id"`
	RunAt       time.Time       `json:"run_at"`
	Queue       string          `json:"queue"`
	Args        json.RawMessage `json:"args"`
	Status      string          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	JobType     string          `json:"job_type"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	ErrorCount  int64           `json:"error_count"`
	ParentID    *string         `json:"parent_id,omitempty"`
}

// Marshal converts a queue.Job into the legacy PubsubJob JSON format.
func (a *LegacyAdapter) Marshal(job *Job) ([]byte, error) {
	now := time.Now().UTC()

	msg := legacyPubsubJob{
		ID:          job.ID,
		RunAt:       job.RunAt.UTC(),
		Queue:       job.Priority.String(),
		Args:        json.RawMessage(job.Payload),
		Status:      "not_published",
		CreatedAt:   job.CreatedAt.UTC(),
		UpdatedAt:   job.CreatedAt.UTC(),
		JobType:     job.Type,
		PublishedAt: &now,
		ErrorCount:  0,
	}

	return json.Marshal(msg)
}

// Ensure LegacyAdapter implements MessageAdapter.
var _ MessageAdapter = (*LegacyAdapter)(nil)
