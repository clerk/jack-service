package api

import (
	"io"
	"log"
	"net/http"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"

	"github.com/clerk/jack-service/internal/queue"
	"github.com/clerk/jack-service/internal/scheduler"
)

// CallbackHandler handles HTTP callbacks from Cloud Tasks for scheduled jobs.
type CallbackHandler struct {
	backend queue.Backend
	statsd  statsd.ClientInterface
}

// NewCallbackHandler creates a new callback handler.
func NewCallbackHandler(backend queue.Backend, sd statsd.ClientInterface) *CallbackHandler {
	if sd == nil {
		sd = &statsd.NoOpClient{}
	}
	return &CallbackHandler{
		backend: backend,
		statsd:  sd,
	}
}

// Register adds the callback routes to the given mux.
func (h *CallbackHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/callback/enqueue", h.handleEnqueueCallback)
}

// handleEnqueueCallback processes a Cloud Tasks callback to enqueue a scheduled job.
func (h *CallbackHandler) handleEnqueueCallback(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		_ = h.statsd.Incr("jack.callback.count", []string{"status:error", "reason:read_body"}, 1)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	job, err := scheduler.UnmarshalJob(body)
	if err != nil {
		_ = h.statsd.Incr("jack.callback.count", []string{"status:error", "reason:unmarshal"}, 1)
		http.Error(w, "failed to parse job", http.StatusBadRequest)
		return
	}

	// The scheduled time has arrived — set RunAt to now for immediate execution.
	job.RunAt = time.Now()

	if err := h.backend.Enqueue(r.Context(), job); err != nil {
		_ = h.statsd.Incr("jack.callback.count", []string{"status:error", "reason:enqueue"}, 1)
		log.Printf("callback: failed to enqueue job %s: %v", job.ID, err)
		// Return 500 so Cloud Tasks retries.
		http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
		return
	}

	_ = h.statsd.Incr("jack.callback.count", []string{"status:success"}, 1)
	_ = h.statsd.Distribution("jack.callback.duration", time.Since(start).Seconds(), nil, 1)

	log.Printf("callback: enqueued scheduled job %s (type=%s, priority=%s)", job.ID, job.Type, job.Priority)
	w.WriteHeader(http.StatusOK)
}
