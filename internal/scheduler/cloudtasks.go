package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/DataDog/datadog-go/v5/statsd"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/clerk/jack-service/internal/queue"
)

// MaxTaskBodySize is the maximum Cloud Tasks body size (100KB) with a safety margin.
const MaxTaskBodySize = 95 * 1024 // 95KB

// CloudTasksConfig contains configuration for the Cloud Tasks scheduler.
type CloudTasksConfig struct {
	Project             string
	Location            string
	Queue               string
	CallbackBaseURL     string
	ServiceAccountEmail string
	Statsd              statsd.ClientInterface
}

// CloudTasksScheduler schedules future jobs via GCP Cloud Tasks.
type CloudTasksScheduler struct {
	client      *cloudtasks.Client
	queuePath   string
	callbackURL string
	saEmail     string
	statsd      statsd.ClientInterface
}

// NewCloudTasksScheduler creates a new Cloud Tasks scheduler.
func NewCloudTasksScheduler(ctx context.Context, cfg CloudTasksConfig) (*CloudTasksScheduler, error) {
	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("scheduler: failed to create Cloud Tasks client: %w", err)
	}

	queuePath := fmt.Sprintf("projects/%s/locations/%s/queues/%s",
		cfg.Project, cfg.Location, cfg.Queue)

	callbackURL := strings.TrimRight(cfg.CallbackBaseURL, "/") + "/internal/callback/enqueue"

	sd := cfg.Statsd
	if sd == nil {
		sd = &statsd.NoOpClient{}
	}

	return &CloudTasksScheduler{
		client:      client,
		queuePath:   queuePath,
		callbackURL: callbackURL,
		saEmail:     cfg.ServiceAccountEmail,
		statsd:      sd,
	}, nil
}

// Schedule creates a Cloud Task that fires at job.RunAt.
func (s *CloudTasksScheduler) Schedule(ctx context.Context, job *queue.Job) error {
	start := time.Now()

	data, err := marshalJob(job)
	if err != nil {
		_ = s.statsd.Incr("jack.schedule.count", []string{"status:error", "reason:marshal"}, 1)
		return fmt.Errorf("scheduler: failed to marshal job: %w", err)
	}

	if len(data) > MaxTaskBodySize {
		_ = s.statsd.Incr("jack.schedule.count", []string{"status:error", "reason:too_large"}, 1)
		return fmt.Errorf("scheduler: serialized job (%d bytes) exceeds Cloud Tasks limit (%d bytes)", len(data), MaxTaskBodySize)
	}

	// Build task name from job ID for deduplication.
	// Cloud Tasks task names must match [a-zA-Z0-9_-].
	taskName := fmt.Sprintf("%s/tasks/%s", s.queuePath, job.ID)

	httpReq := &cloudtaskspb.HttpRequest{
		HttpMethod: cloudtaskspb.HttpMethod_POST,
		Url:        s.callbackURL,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       data,
	}

	// Add OIDC token for Cloud Run authentication if service account is configured.
	if s.saEmail != "" {
		httpReq.AuthorizationHeader = &cloudtaskspb.HttpRequest_OidcToken{
			OidcToken: &cloudtaskspb.OidcToken{
				ServiceAccountEmail: s.saEmail,
				Audience:            s.callbackURL,
			},
		}
	}

	req := &cloudtaskspb.CreateTaskRequest{
		Parent: s.queuePath,
		Task: &cloudtaskspb.Task{
			Name:         taskName,
			ScheduleTime: timestamppb.New(job.RunAt),
			MessageType: &cloudtaskspb.Task_HttpRequest{
				HttpRequest: httpReq,
			},
		},
	}

	if _, err := s.client.CreateTask(ctx, req); err != nil {
		_ = s.statsd.Incr("jack.schedule.count", []string{"status:error", "reason:create"}, 1)
		return fmt.Errorf("scheduler: failed to create task: %w", err)
	}

	_ = s.statsd.Incr("jack.schedule.count", []string{"status:success"}, 1)
	_ = s.statsd.Distribution("jack.schedule.duration", time.Since(start).Seconds(), nil, 1)

	return nil
}

// ScheduleBulk schedules multiple jobs in parallel.
func (s *CloudTasksScheduler) ScheduleBulk(ctx context.Context, jobs []*queue.Job) []queue.EnqueueResult {
	results := make([]queue.EnqueueResult, len(jobs))
	var wg sync.WaitGroup

	for i, job := range jobs {
		wg.Add(1)
		go func(idx int, j *queue.Job) {
			defer wg.Done()
			if err := s.Schedule(ctx, j); err != nil {
				results[idx] = queue.EnqueueResult{Error: err}
			} else {
				results[idx] = queue.EnqueueResult{JobID: j.ID}
			}
		}(i, job)
	}

	wg.Wait()
	return results
}

// Close shuts down the Cloud Tasks client.
func (s *CloudTasksScheduler) Close() error {
	return s.client.Close()
}

// Ensure CloudTasksScheduler implements Scheduler.
var _ Scheduler = (*CloudTasksScheduler)(nil)
