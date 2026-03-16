package queue

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/DataDog/datadog-go/v5/statsd"
)

// PubSubBackend publishes jobs to GCP Pub/Sub topics, one per priority level.
// It delegates message serialization to a MessageAdapter, keeping the backend
// format-agnostic.
type PubSubBackend struct {
	client  *pubsub.Client
	topics  map[Priority]*pubsub.Topic
	adapter MessageAdapter
	statsd  statsd.ClientInterface
}

// PubSubConfig contains configuration for the Pub/Sub backend.
type PubSubConfig struct {
	// Project is the GCP project ID.
	Project string

	// Topic names per priority level.
	HighTopic      string
	MediumTopic    string
	LowTopic       string
	ImmediateTopic string

	// Adapter converts queue.Job into the wire format for the consumer.
	Adapter MessageAdapter

	// Statsd is the DogStatsD client for metrics.
	Statsd statsd.ClientInterface
}

// NewPubSubBackend creates a new Pub/Sub queue backend.
func NewPubSubBackend(ctx context.Context, cfg PubSubConfig) (*PubSubBackend, error) {
	if cfg.Project == "" {
		return nil, fmt.Errorf("pubsub: project is required")
	}
	if cfg.Adapter == nil {
		return nil, fmt.Errorf("pubsub: adapter is required")
	}

	client, err := pubsub.NewClient(ctx, cfg.Project)
	if err != nil {
		return nil, fmt.Errorf("pubsub: create client: %w", err)
	}

	topics := make(map[Priority]*pubsub.Topic)

	if cfg.HighTopic != "" {
		topics[PriorityHigh] = client.Topic(cfg.HighTopic)
	}
	if cfg.MediumTopic != "" {
		topics[PriorityMedium] = client.Topic(cfg.MediumTopic)
	}
	if cfg.LowTopic != "" {
		topics[PriorityLow] = client.Topic(cfg.LowTopic)
	}
	if cfg.ImmediateTopic != "" {
		topics[PriorityImmediate] = client.Topic(cfg.ImmediateTopic)
	}

	sd := cfg.Statsd
	if sd == nil {
		sd = &statsd.NoOpClient{}
	}

	return &PubSubBackend{
		client:  client,
		topics:  topics,
		adapter: cfg.Adapter,
		statsd:  sd,
	}, nil
}

// topicFor returns the Pub/Sub topic for the given priority.
// Falls back to medium if no topic is configured for the priority.
func (b *PubSubBackend) topicFor(p Priority) (*pubsub.Topic, error) {
	if topic, ok := b.topics[p]; ok {
		return topic, nil
	}
	// Fall back to medium queue
	if topic, ok := b.topics[PriorityMedium]; ok {
		return topic, nil
	}
	return nil, fmt.Errorf("pubsub: no topic configured for priority %s", p)
}

// Enqueue publishes a single job to the appropriate Pub/Sub topic.
func (b *PubSubBackend) Enqueue(ctx context.Context, job *Job) error {
	topic, err := b.topicFor(job.Priority)
	if err != nil {
		return err
	}

	data, err := b.adapter.Marshal(job)
	if err != nil {
		return fmt.Errorf("pubsub: marshal job %s: %w", job.ID, err)
	}

	tags := []string{"priority:" + job.Priority.String()}

	attrs := map[string]string{
		"shadow": strconv.FormatBool(job.Shadow),
	}

	publishStart := time.Now()
	result := topic.Publish(ctx, &pubsub.Message{Data: data, Attributes: attrs})
	if _, err := result.Get(ctx); err != nil {
		_ = b.statsd.Incr("jack.publish.count", append(tags, "status:error"), 1)
		return fmt.Errorf("pubsub: publish job %s: %w", job.ID, err)
	}

	_ = b.statsd.Incr("jack.publish.count", append(tags, "status:success"), 1)
	_ = b.statsd.Distribution("jack.publish.duration", time.Since(publishStart).Seconds(), tags, 1)

	log.Printf("[pubsub] published job: id=%s type=%s priority=%s",
		job.ID, job.Type, job.Priority)

	return nil
}

// EnqueueBulk publishes multiple jobs asynchronously and collects results.
func (b *PubSubBackend) EnqueueBulk(ctx context.Context, jobs []*Job) []EnqueueResult {
	results := make([]EnqueueResult, len(jobs))

	type publishResult struct {
		index  int
		result *pubsub.PublishResult
		err    error
	}

	// Publish all jobs asynchronously
	pending := make([]publishResult, len(jobs))
	for i, job := range jobs {
		topic, err := b.topicFor(job.Priority)
		if err != nil {
			pending[i] = publishResult{index: i, err: err}
			continue
		}

		data, err := b.adapter.Marshal(job)
		if err != nil {
			pending[i] = publishResult{index: i, err: fmt.Errorf("marshal: %w", err)}
			continue
		}

		attrs := map[string]string{
			"shadow": strconv.FormatBool(job.Shadow),
		}

		pr := topic.Publish(ctx, &pubsub.Message{Data: data, Attributes: attrs})
		pending[i] = publishResult{index: i, result: pr}
	}

	// Wait for all results
	var wg sync.WaitGroup
	for i, p := range pending {
		if p.err != nil {
			results[i] = EnqueueResult{
				JobID: jobs[i].ID,
				Error: p.err,
			}
			continue
		}

		wg.Add(1)
		go func(idx int, pr *pubsub.PublishResult) {
			defer wg.Done()
			if _, err := pr.Get(ctx); err != nil {
				results[idx] = EnqueueResult{
					JobID: jobs[idx].ID,
					Error: fmt.Errorf("pubsub: publish: %w", err),
				}
			} else {
				results[idx] = EnqueueResult{
					JobID: jobs[idx].ID,
				}
			}
		}(i, p.result)
	}
	wg.Wait()

	// Emit per-job publish metrics
	for i, r := range results {
		tags := []string{"priority:" + jobs[i].Priority.String()}
		if r.Error != nil {
			_ = b.statsd.Incr("jack.publish.count", append(tags, "status:error"), 1)
		} else {
			_ = b.statsd.Incr("jack.publish.count", append(tags, "status:success"), 1)
		}
	}

	return results
}

// Health checks if the Pub/Sub client is healthy by verifying topic existence.
func (b *PubSubBackend) Health(ctx context.Context) error {
	for priority, topic := range b.topics {
		exists, err := topic.Exists(ctx)
		if err != nil {
			return fmt.Errorf("pubsub: health check for %s topic: %w", priority, err)
		}
		if !exists {
			return fmt.Errorf("pubsub: topic %s for %s does not exist", topic.ID(), priority)
		}
	}
	return nil
}

// Close stops all topics and closes the Pub/Sub client.
func (b *PubSubBackend) Close() error {
	for _, topic := range b.topics {
		topic.Stop()
	}
	return b.client.Close()
}

// Ensure PubSubBackend implements Backend.
var _ Backend = (*PubSubBackend)(nil)
