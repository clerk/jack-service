package queue

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLegacyAdapterMarshal(t *testing.T) {
	adapter := &LegacyAdapter{}

	job := &Job{
		ID:         "job_abc123",
		ProducerID: "clerk-go",
		Type:       "SendEmail",
		Priority:   PriorityHigh,
		Payload:    []byte(`{"TraceID":"trace-xyz","Args":{"InstanceID":"inst_1","EmailID":"email_2"}}`),
		RunAt:      time.Date(2026, 2, 16, 10, 30, 0, 0, time.UTC),
		TraceID:    "trace-xyz",
		MaxRetries: 10,
		CreatedAt:  time.Date(2026, 2, 16, 10, 25, 0, 0, time.UTC),
	}

	data, err := adapter.Marshal(job)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Unmarshal into a generic map to verify field names and values.
	var result map[string]json.RawMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal result failed: %v", err)
	}

	// Check required fields exist
	requiredFields := []string{"id", "run_at", "queue", "args", "status", "created_at", "updated_at", "job_type", "error_count"}
	for _, field := range requiredFields {
		if _, ok := result[field]; !ok {
			t.Errorf("missing required field: %s", field)
		}
	}

	// Unmarshal into the exact struct to verify values
	var msg legacyPubsubJob
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Unmarshal into legacyPubsubJob failed: %v", err)
	}

	if msg.ID != "job_abc123" {
		t.Errorf("ID = %q, want %q", msg.ID, "job_abc123")
	}
	if msg.JobType != "SendEmail" {
		t.Errorf("JobType = %q, want %q", msg.JobType, "SendEmail")
	}
	if msg.Queue != "high" {
		t.Errorf("Queue = %q, want %q", msg.Queue, "high")
	}
	if msg.Status != "not_published" {
		t.Errorf("Status = %q, want %q", msg.Status, "not_published")
	}
	if msg.ErrorCount != 0 {
		t.Errorf("ErrorCount = %d, want 0", msg.ErrorCount)
	}
	if msg.LastError != nil {
		t.Errorf("LastError = %v, want nil", msg.LastError)
	}
	if msg.ParentID != nil {
		t.Errorf("ParentID = %v, want nil", msg.ParentID)
	}
	if msg.PublishedAt == nil {
		t.Error("PublishedAt should be set")
	}
	if !msg.RunAt.Equal(time.Date(2026, 2, 16, 10, 30, 0, 0, time.UTC)) {
		t.Errorf("RunAt = %v, want 2026-02-16T10:30:00Z", msg.RunAt)
	}
	if !msg.CreatedAt.Equal(time.Date(2026, 2, 16, 10, 25, 0, 0, time.UTC)) {
		t.Errorf("CreatedAt = %v, want 2026-02-16T10:25:00Z", msg.CreatedAt)
	}
	if !msg.UpdatedAt.Equal(msg.CreatedAt) {
		t.Errorf("UpdatedAt = %v, want same as CreatedAt %v", msg.UpdatedAt, msg.CreatedAt)
	}

	// Verify args is passed through as raw JSON
	var args map[string]json.RawMessage
	if err := json.Unmarshal(msg.Args, &args); err != nil {
		t.Fatalf("Unmarshal args failed: %v", err)
	}
	if _, ok := args["TraceID"]; !ok {
		t.Error("args should contain TraceID field")
	}
	if _, ok := args["Args"]; !ok {
		t.Error("args should contain Args field")
	}
}

func TestLegacyAdapterPriorityMapping(t *testing.T) {
	adapter := &LegacyAdapter{}

	tests := []struct {
		priority Priority
		want     string
	}{
		{PriorityImmediate, "immediate"},
		{PriorityHigh, "high"},
		{PriorityMedium, "medium"},
		{PriorityLow, "low"},
	}

	for _, tt := range tests {
		job := &Job{
			ID:        "test-id",
			Type:      "TestJob",
			Priority:  tt.priority,
			Payload:   []byte(`{}`),
			RunAt:     time.Now(),
			CreatedAt: time.Now(),
		}

		data, err := adapter.Marshal(job)
		if err != nil {
			t.Fatalf("Marshal with priority %v failed: %v", tt.priority, err)
		}

		var msg legacyPubsubJob
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		if msg.Queue != tt.want {
			t.Errorf("Priority %v: Queue = %q, want %q", tt.priority, msg.Queue, tt.want)
		}
	}
}

func TestLegacyAdapterNilPayload(t *testing.T) {
	adapter := &LegacyAdapter{}

	job := &Job{
		ID:        "test-id",
		Type:      "TestJob",
		Priority:  PriorityMedium,
		Payload:   nil,
		RunAt:     time.Now(),
		CreatedAt: time.Now(),
	}

	data, err := adapter.Marshal(job)
	if err != nil {
		t.Fatalf("Marshal with nil payload failed: %v", err)
	}

	var msg legacyPubsubJob
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	// nil payload should produce null JSON
	if string(msg.Args) != "null" {
		t.Errorf("Args = %s, want null for nil payload", string(msg.Args))
	}
}
