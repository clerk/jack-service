package queue

import (
	"testing"
	"time"
)

func TestPubSubBackendTopicRouting(t *testing.T) {
	// Test topicFor fallback behavior without creating a real Pub/Sub client.
	// We can't easily create real topics in unit tests, so we test the
	// adapter integration and config validation instead.

	t.Run("config requires project", func(t *testing.T) {
		_, err := NewPubSubBackend(t.Context(), PubSubConfig{
			Adapter: &LegacyAdapter{},
		})
		if err == nil {
			t.Error("expected error for empty project")
		}
	})

	t.Run("config requires adapter", func(t *testing.T) {
		_, err := NewPubSubBackend(t.Context(), PubSubConfig{
			Project: "test-project",
		})
		if err == nil {
			t.Error("expected error for nil adapter")
		}
	})
}

func TestPubSubBackendAdapterIntegration(t *testing.T) {
	// Verify that the adapter produces valid output for various job configurations.
	adapter := &LegacyAdapter{}

	jobs := []*Job{
		{
			ID: "job-1", Type: "SendEmail", Priority: PriorityHigh,
			Payload: []byte(`{"TraceID":"t1","Args":{"email":"a@b.com"}}`),
			RunAt: time.Now(), CreatedAt: time.Now(),
		},
		{
			ID: "job-2", Type: "CheckDNS", Priority: PriorityLow,
			Payload: []byte(`{"TraceID":"t2","Args":{}}`),
			RunAt: time.Now(), CreatedAt: time.Now(),
		},
		{
			ID: "job-3", Type: "DispatchWebhook", Priority: PriorityMedium,
			Payload: []byte(`{"TraceID":"t3","Args":{"url":"https://example.com"}}`),
			RunAt: time.Now(), CreatedAt: time.Now(),
		},
	}

	for _, job := range jobs {
		data, err := adapter.Marshal(job)
		if err != nil {
			t.Errorf("Marshal job %s failed: %v", job.ID, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("Marshal job %s produced empty data", job.ID)
		}
	}
}
