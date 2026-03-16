// Package api implements the gRPC server for the Jack service.
package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/clerk/jack-service/internal/jobid"
	"github.com/clerk/jack-service/internal/queue"
	"github.com/clerk/jack-service/internal/scheduler"
	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/proto/jackpb"
)

// Server implements the BackgroundJobs gRPC service.
type Server struct {
	jackpb.UnimplementedBackgroundJobsServer

	store     storage.Store
	backend   queue.Backend
	scheduler scheduler.Scheduler
	config    ServerConfig
	statsd    statsd.ClientInterface
}

// ServerConfig contains configuration for the gRPC server.
type ServerConfig struct {
	// MaxPayloadSize is the maximum allowed payload size in bytes.
	MaxPayloadSize int

	// DefaultQueue is the queue to use when job type is not configured.
	DefaultQueue queue.Priority

	// DefaultMaxRetries is the retry count when job type is not configured.
	DefaultMaxRetries int

	// ScheduleThreshold is the minimum RunAt offset to trigger Cloud Tasks scheduling.
	// Jobs with RunAt >= now + ScheduleThreshold are scheduled via Cloud Tasks.
	ScheduleThreshold time.Duration
}

// DefaultServerConfig returns the default server configuration.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		MaxPayloadSize:    1048576, // 1MB
		DefaultQueue:      queue.PriorityMedium,
		DefaultMaxRetries: 10,
	}
}

// NewServer creates a new gRPC server.
func NewServer(store storage.Store, backend queue.Backend, sched scheduler.Scheduler, config ServerConfig, sd statsd.ClientInterface) *Server {
	return &Server{
		store:     store,
		backend:   backend,
		scheduler: sched,
		config:    config,
		statsd:    sd,
	}
}

// Enqueue handles a single job enqueue request.
func (s *Server) Enqueue(ctx context.Context, req *jackpb.EnqueueRequest) (*jackpb.EnqueueResponse, error) {
	start := time.Now()

	// Validate required fields
	if req.ProducerId == "" {
		return nil, status.Error(codes.InvalidArgument, "producer_id is required")
	}
	if req.JobType == "" {
		return nil, status.Error(codes.InvalidArgument, "job_type is required")
	}
	if len(req.Payload) > s.config.MaxPayloadSize {
		return nil, status.Errorf(codes.InvalidArgument, "payload exceeds max size (%d bytes)", s.config.MaxPayloadSize)
	}

	var warnings []string

	// Check if producer is registered (warn if not)
	if s.store != nil {
		if _, err := s.store.GetProducer(ctx, req.ProducerId); err != nil {
			warnings = append(warnings, fmt.Sprintf("unregistered producer_id: %s", req.ProducerId))
		}
	}

	// Look up job type configuration (collects warning if not found)
	priority, maxRetries, jobTypeWarning := s.getJobTypeConfig(ctx, req.ProducerId, req.JobType)
	if jobTypeWarning != "" {
		warnings = append(warnings, jobTypeWarning)
	}

	// Generate job ID
	jobID := jobid.Generate(req.ProducerId, req.JobType)

	// Determine run time
	runAt := time.Now()
	if req.RunAt != nil {
		runAt = req.RunAt.AsTime()
	}

	// Read shadow flag from gRPC metadata
	shadow := shadowFromContext(ctx)

	// Create job
	job := &queue.Job{
		ID:         jobID,
		ProducerID: req.ProducerId,
		Type:       req.JobType,
		Priority:   priority,
		Payload:    req.Payload,
		RunAt:      runAt,
		TraceID:    req.TraceId,
		MaxRetries: maxRetries,
		CreatedAt:  time.Now(),
		Shadow:     shadow,
	}

	// Enqueue to backend (or schedule for the future)
	tags := []string{
		"job_type:" + req.JobType,
		"producer_id:" + req.ProducerId,
		"priority:" + priority.String(),
	}

	// If the job is scheduled far enough in the future, use Cloud Tasks.
	if s.scheduler != nil && s.config.ScheduleThreshold > 0 && time.Until(runAt) >= s.config.ScheduleThreshold {
		if err := s.scheduler.Schedule(ctx, job); err != nil {
			_ = s.statsd.Incr("jack.enqueue.count", append(tags, "status:error", "method:scheduled"), 1)
			return nil, status.Error(codes.Internal, "failed to schedule future job")
		}

		_ = s.statsd.Incr("jack.enqueue.count", append(tags, "status:success", "method:scheduled"), 1)
		_ = s.statsd.Distribution("jack.enqueue.duration", time.Since(start).Seconds(), tags, 1)
		_ = s.statsd.Distribution("jack.enqueue.payload_bytes", float64(len(req.Payload)), tags[:2], 1)

		return &jackpb.EnqueueResponse{
			JobId:         jobID,
			ErrorMessages: warnings,
			CorrelationId: req.CorrelationId,
		}, nil
	}

	if err := s.backend.Enqueue(ctx, job); err != nil {
		_ = s.statsd.Incr("jack.enqueue.count", append(tags, "status:error", "method:immediate"), 1)
		if errors.Is(err, queue.ErrQueueUnavailable) {
			return nil, status.Error(codes.Unavailable, "queue temporarily unavailable")
		}
		return nil, status.Error(codes.Internal, "failed to enqueue job")
	}

	_ = s.statsd.Incr("jack.enqueue.count", append(tags, "status:success", "method:immediate"), 1)
	_ = s.statsd.Distribution("jack.enqueue.duration", time.Since(start).Seconds(), tags, 1)
	_ = s.statsd.Distribution("jack.enqueue.payload_bytes", float64(len(req.Payload)), tags[:2], 1)

	return &jackpb.EnqueueResponse{
		JobId:         jobID,
		ErrorMessages: warnings,
		CorrelationId: req.CorrelationId,
	}, nil
}

// EnqueueBulk handles a bulk job enqueue request.
func (s *Server) EnqueueBulk(ctx context.Context, req *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
	start := time.Now()

	if len(req.Jobs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "jobs is required")
	}

	// Read shadow flag from gRPC metadata (applies to all jobs in the batch)
	shadow := shadowFromContext(ctx)

	// Prepare all jobs
	jobs := make([]*queue.Job, len(req.Jobs))
	validationErrors := make([]string, len(req.Jobs))
	jobWarnings := make([][]string, len(req.Jobs))

	for i, r := range req.Jobs {
		// Validate
		if r.ProducerId == "" {
			validationErrors[i] = "producer_id is required"
			continue
		}
		if r.JobType == "" {
			validationErrors[i] = "job_type is required"
			continue
		}
		if len(r.Payload) > s.config.MaxPayloadSize {
			validationErrors[i] = "payload exceeds max size"
			continue
		}

		var warnings []string

		// Check if producer is registered (warn if not)
		if s.store != nil {
			if _, err := s.store.GetProducer(ctx, r.ProducerId); err != nil {
				warnings = append(warnings, fmt.Sprintf("unregistered producer_id: %s", r.ProducerId))
			}
		}

		// Look up job type configuration
		priority, maxRetries, jobTypeWarning := s.getJobTypeConfig(ctx, r.ProducerId, r.JobType)
		if jobTypeWarning != "" {
			warnings = append(warnings, jobTypeWarning)
		}

		jobWarnings[i] = warnings

		// Generate job ID
		jobID := jobid.Generate(r.ProducerId, r.JobType)

		// Determine run time
		runAt := time.Now()
		if r.RunAt != nil {
			runAt = r.RunAt.AsTime()
		}

		jobs[i] = &queue.Job{
			ID:         jobID,
			ProducerID: r.ProducerId,
			Type:       r.JobType,
			Priority:   priority,
			Payload:    r.Payload,
			RunAt:      runAt,
			TraceID:    r.TraceId,
			MaxRetries: maxRetries,
			CreatedAt:  time.Now(),
			Shadow:     shadow,
		}
	}

	// Partition valid jobs into immediate and future (scheduled).
	var immediateJobs []*queue.Job
	var futureJobs []*queue.Job
	immediateIndexes := make([]int, 0)
	futureIndexes := make([]int, 0)
	useScheduler := s.scheduler != nil && s.config.ScheduleThreshold > 0

	for i, job := range jobs {
		if job == nil {
			continue
		}
		if useScheduler && time.Until(job.RunAt) >= s.config.ScheduleThreshold {
			futureJobs = append(futureJobs, job)
			futureIndexes = append(futureIndexes, i)
		} else {
			immediateJobs = append(immediateJobs, job)
			immediateIndexes = append(immediateIndexes, i)
		}
	}

	// Bulk enqueue immediate jobs to backend.
	var immediateResults []queue.EnqueueResult
	if len(immediateJobs) > 0 {
		immediateResults = s.backend.EnqueueBulk(ctx, immediateJobs)
	}

	// Schedule future jobs via Cloud Tasks.
	var futureResults []queue.EnqueueResult
	if len(futureJobs) > 0 {
		futureResults = s.scheduler.ScheduleBulk(ctx, futureJobs)
	}

	// Build response
	results := make([]*jackpb.BulkResult, len(req.Jobs))
	for i, r := range req.Jobs {
		results[i] = &jackpb.BulkResult{
			Index:         int32(i),
			ErrorMessages: jobWarnings[i],
			CorrelationId: r.CorrelationId,
		}
	}

	// Fill in validation errors
	for i, errMsg := range validationErrors {
		if errMsg != "" {
			results[i].Error = errMsg
		}
	}

	// Fill in immediate backend results.
	for idx, validIdx := range immediateIndexes {
		if idx < len(immediateResults) {
			br := immediateResults[idx]
			if br.Error != nil {
				results[validIdx].Error = br.Error.Error()
			} else {
				results[validIdx].JobId = br.JobID
			}
		}
	}

	// Fill in scheduled results.
	for idx, validIdx := range futureIndexes {
		if idx < len(futureResults) {
			br := futureResults[idx]
			if br.Error != nil {
				results[validIdx].Error = br.Error.Error()
			} else {
				results[validIdx].JobId = br.JobID
			}
		}
	}

	// Emit bulk metrics
	_ = s.statsd.Distribution("jack.enqueue_bulk.jobs", float64(len(req.Jobs)), nil, 1)
	_ = s.statsd.Distribution("jack.enqueue_bulk.duration", time.Since(start).Seconds(), nil, 1)
	for _, br := range immediateResults {
		st := "success"
		if br.Error != nil {
			st = "error"
		}
		_ = s.statsd.Incr("jack.enqueue_bulk.count", []string{"status:" + st, "method:immediate"}, 1)
	}
	for _, br := range futureResults {
		st := "success"
		if br.Error != nil {
			st = "error"
		}
		_ = s.statsd.Incr("jack.enqueue_bulk.count", []string{"status:" + st, "method:scheduled"}, 1)
	}

	return &jackpb.EnqueueBulkResponse{
		Results: results,
	}, nil
}

// getJobTypeConfig looks up the queue and max retries for a job type.
// Returns defaults and a warning message if not found.
func (s *Server) getJobTypeConfig(ctx context.Context, producerID, jobType string) (queue.Priority, int, string) {
	if s.store == nil {
		return s.config.DefaultQueue, s.config.DefaultMaxRetries, ""
	}

	jt, err := s.store.GetJobType(ctx, producerID, jobType)
	if err != nil {
		warning := fmt.Sprintf("unregistered job_type: %s (producer: %s)", jobType, producerID)
		return s.config.DefaultQueue, s.config.DefaultMaxRetries, warning
	}

	return storageQueueToQueuePriority(jt.Queue), int(jt.MaxRetries), ""
}

// shadowFromContext reads the "shadow" flag from gRPC metadata.
// Returns false if the header is missing or not "true".
func shadowFromContext(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get("shadow")
	return len(vals) > 0 && vals[0] == "true"
}

// storageQueueToQueuePriority converts a storage queue to a queue.Priority.
func storageQueueToQueuePriority(q jackpb.Queue) queue.Priority {
	switch q {
	case jackpb.Queue_QUEUE_IMMEDIATE:
		return queue.PriorityImmediate
	case jackpb.Queue_QUEUE_HIGH:
		return queue.PriorityHigh
	case jackpb.Queue_QUEUE_MEDIUM:
		return queue.PriorityMedium
	case jackpb.Queue_QUEUE_LOW:
		return queue.PriorityLow
	default:
		return queue.PriorityMedium // Default to medium
	}
}
