//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/DataDog/datadog-go/v5/statsd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/clerk/jack-service/internal/api"
	"github.com/clerk/jack-service/internal/queue"
	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/proto/jackpb"
)

const (
	testProject  = "test-project"
	topicHigh    = "jack-high"
	topicMedium  = "jack-medium"
	topicLow     = "jack-low"
	topicImm     = "jack-immediate"
	emulatorAddr = "localhost:8085"
)

var (
	bgClient     jackpb.BackgroundJobsClient
	adminClient  jackpb.AdminServiceClient
	healthClient healthpb.HealthClient
	psClient     *pubsub.Client

	subHigh   *pubsub.Subscription
	subMedium *pubsub.Subscription
	subLow    *pubsub.Subscription
	subImm    *pubsub.Subscription
)

func TestMain(m *testing.M) {
	emulatorCmd := startEmulatorIfNeeded()

	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorAddr)

	ctx := context.Background()

	var err error
	psClient, err = pubsub.NewClient(ctx, testProject)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	createTopicsAndSubs(ctx)

	store := storage.NewMemoryStore()
	backend, err := queue.NewPubSubBackend(ctx, queue.PubSubConfig{
		Project:        testProject,
		HighTopic:      topicHigh,
		MediumTopic:    topicMedium,
		LowTopic:       topicLow,
		ImmediateTopic: topicImm,
		Adapter:        &queue.LegacyAdapter{},
		Statsd:         &statsd.NoOpClient{},
	})
	if err != nil {
		log.Fatalf("NewPubSubBackend: %v", err)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatalf("net.Listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	jackpb.RegisterBackgroundJobsServer(grpcServer,
		api.NewServer(store, backend, api.DefaultServerConfig(), &statsd.NoOpClient{}))
	jackpb.RegisterAdminServiceServer(grpcServer, api.NewAdminServer(store))

	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	go grpcServer.Serve(lis)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc.NewClient: %v", err)
	}

	bgClient = jackpb.NewBackgroundJobsClient(conn)
	adminClient = jackpb.NewAdminServiceClient(conn)
	healthClient = healthpb.NewHealthClient(conn)

	code := m.Run()

	conn.Close()
	grpcServer.GracefulStop()
	backend.Close()
	psClient.Close()
	if emulatorCmd != nil {
		emulatorCmd.Process.Kill()
	}
	os.Exit(code)
}

// --- Tests ---

func TestEnqueue_ArrivesOnCorrectTopic(t *testing.T) {
	ctx := context.Background()
	drainSub(ctx, subMedium)

	payload := []byte(`{"TraceID":"trace-1","Args":{"email":"test@example.com"}}`)
	resp, err := bgClient.Enqueue(ctx, &jackpb.EnqueueRequest{
		ProducerId:    "test-producer",
		JobType:       "SendEmail",
		Payload:       payload,
		TraceId:       "trace-1",
		CorrelationId: "corr-1",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if resp.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}
	if resp.CorrelationId != "corr-1" {
		t.Errorf("correlation_id = %q, want %q", resp.CorrelationId, "corr-1")
	}

	msg, err := pullOne(ctx, subMedium, 5*time.Second)
	if err != nil {
		t.Fatalf("pullOne: %v", err)
	}

	var legacy map[string]json.RawMessage
	if err := json.Unmarshal(msg.Data, &legacy); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}

	var jobType string
	json.Unmarshal(legacy["job_type"], &jobType)
	if jobType != "SendEmail" {
		t.Errorf("job_type = %q, want %q", jobType, "SendEmail")
	}

	var queueName string
	json.Unmarshal(legacy["queue"], &queueName)
	if queueName != "medium" {
		t.Errorf("queue = %q, want %q", queueName, "medium")
	}

	if string(legacy["args"]) != string(payload) {
		t.Errorf("args = %s, want %s", legacy["args"], payload)
	}
}

func TestEnqueueBulk_AllMessagesDelivered(t *testing.T) {
	ctx := context.Background()
	drainSub(ctx, subMedium)

	resp, err := bgClient.EnqueueBulk(ctx, &jackpb.EnqueueBulkRequest{
		Jobs: []*jackpb.EnqueueRequest{
			{ProducerId: "prod-a", JobType: "Job1", Payload: []byte(`{"Args":{}}`)},
			{ProducerId: "prod-a", JobType: "Job2", Payload: []byte(`{"Args":{}}`)},
			{ProducerId: "prod-b", JobType: "Job3", Payload: []byte(`{"Args":{}}`)},
		},
	})
	if err != nil {
		t.Fatalf("EnqueueBulk: %v", err)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("got %d results, want 3", len(resp.Results))
	}
	for i, r := range resp.Results {
		if r.JobId == "" {
			t.Errorf("result[%d]: empty job_id (error=%s)", i, r.Error)
		}
	}

	msgs, err := pullN(ctx, subMedium, 3, 5*time.Second)
	if err != nil {
		t.Fatalf("pullN: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3", len(msgs))
	}
}

func TestPriorityRouting(t *testing.T) {
	ctx := context.Background()
	drainSub(ctx, subHigh)
	drainSub(ctx, subLow)
	drainSub(ctx, subImm)

	prodResp, err := adminClient.CreateProducer(ctx, &jackpb.CreateProducerRequest{
		Name: "priority-test-producer",
	})
	if err != nil {
		t.Fatalf("CreateProducer: %v", err)
	}
	producerID := prodResp.Producer.ProducerId

	priorities := []struct {
		name  string
		queue jackpb.Queue
		sub   *pubsub.Subscription
		label string
	}{
		{"HighJob", jackpb.Queue_QUEUE_HIGH, subHigh, "high"},
		{"LowJob", jackpb.Queue_QUEUE_LOW, subLow, "low"},
		{"ImmJob", jackpb.Queue_QUEUE_IMMEDIATE, subImm, "immediate"},
	}

	for _, p := range priorities {
		_, err := adminClient.CreateJobType(ctx, &jackpb.CreateJobTypeRequest{
			ProducerId: producerID,
			Name:       p.name,
			Queue:      p.queue,
			MaxRetries: 5,
		})
		if err != nil {
			t.Fatalf("CreateJobType(%s): %v", p.name, err)
		}
	}

	for _, p := range priorities {
		_, err := bgClient.Enqueue(ctx, &jackpb.EnqueueRequest{
			ProducerId: producerID,
			JobType:    p.name,
			Payload:    []byte(`{"Args":{}}`),
		})
		if err != nil {
			t.Fatalf("Enqueue(%s): %v", p.name, err)
		}
	}

	for _, p := range priorities {
		msg, err := pullOne(ctx, p.sub, 5*time.Second)
		if err != nil {
			t.Fatalf("pull from %s: %v", p.label, err)
		}
		var legacy map[string]json.RawMessage
		json.Unmarshal(msg.Data, &legacy)
		var queueName string
		json.Unmarshal(legacy["queue"], &queueName)
		if queueName != p.label {
			t.Errorf("%s topic: queue = %q, want %q", p.label, queueName, p.label)
		}
	}
}

func TestFallback_UnregisteredJobType(t *testing.T) {
	ctx := context.Background()
	drainSub(ctx, subMedium)

	resp, err := bgClient.Enqueue(ctx, &jackpb.EnqueueRequest{
		ProducerId: "unknown-producer",
		JobType:    "UnknownJob",
		Payload:    []byte(`{"Args":{}}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if resp.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}
	if len(resp.ErrorMessages) == 0 {
		t.Error("expected warnings for unregistered producer/job type")
	}

	msg, err := pullOne(ctx, subMedium, 5*time.Second)
	if err != nil {
		t.Fatalf("pullOne(medium): %v", err)
	}
	var legacy map[string]json.RawMessage
	json.Unmarshal(msg.Data, &legacy)
	var queueName string
	json.Unmarshal(legacy["queue"], &queueName)
	if queueName != "medium" {
		t.Errorf("queue = %q, want %q", queueName, "medium")
	}
}

func TestAdminFlow_EndToEnd(t *testing.T) {
	ctx := context.Background()
	drainSub(ctx, subHigh)

	// Create producer
	prodResp, err := adminClient.CreateProducer(ctx, &jackpb.CreateProducerRequest{
		Name:        "admin-flow-producer",
		Description: "Integration test producer",
	})
	if err != nil {
		t.Fatalf("CreateProducer: %v", err)
	}
	producer := prodResp.Producer
	if producer.ProducerId == "" {
		t.Fatal("producer_id should be generated")
	}

	// Verify GetProducer
	getResp, err := adminClient.GetProducer(ctx, &jackpb.GetProducerRequest{
		ProducerId: producer.ProducerId,
	})
	if err != nil {
		t.Fatalf("GetProducer: %v", err)
	}
	if getResp.Producer.Name != "admin-flow-producer" {
		t.Errorf("GetProducer name = %q", getResp.Producer.Name)
	}

	// Create a HIGH priority job type
	jtResp, err := adminClient.CreateJobType(ctx, &jackpb.CreateJobTypeRequest{
		ProducerId: producer.ProducerId,
		Name:       "HighPriorityEmail",
		Queue:      jackpb.Queue_QUEUE_HIGH,
		MaxRetries: 7,
	})
	if err != nil {
		t.Fatalf("CreateJobType: %v", err)
	}
	if jtResp.JobType.MaxRetries != 7 {
		t.Errorf("max_retries = %d, want 7", jtResp.JobType.MaxRetries)
	}

	// Enqueue using registered producer + job type
	enqResp, err := bgClient.Enqueue(ctx, &jackpb.EnqueueRequest{
		ProducerId: producer.ProducerId,
		JobType:    "HighPriorityEmail",
		Payload:    []byte(`{"Args":{"to":"user@example.com"}}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if len(enqResp.ErrorMessages) > 0 {
		t.Errorf("unexpected warnings: %v", enqResp.ErrorMessages)
	}

	// Message should arrive on the HIGH topic
	msg, err := pullOne(ctx, subHigh, 5*time.Second)
	if err != nil {
		t.Fatalf("pullOne(high): %v", err)
	}
	var legacy map[string]json.RawMessage
	json.Unmarshal(msg.Data, &legacy)
	var queueName string
	json.Unmarshal(legacy["queue"], &queueName)
	if queueName != "high" {
		t.Errorf("queue = %q, want %q", queueName, "high")
	}
}

func TestHealthCheck(t *testing.T) {
	ctx := context.Background()

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health status = %v, want SERVING", resp.Status)
	}
}

// --- Helpers ---

func startEmulatorIfNeeded() *exec.Cmd {
	if os.Getenv("PUBSUB_EMULATOR_HOST") != "" {
		return nil
	}
	cmd := exec.Command("gcloud", "beta", "emulators", "pubsub", "start",
		"--host-port="+emulatorAddr)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("start emulator: %v", err)
	}
	waitForPort(emulatorAddr, 15*time.Second)
	return cmd
}

func waitForPort(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("emulator at %s not ready after %v", addr, timeout)
}

func createTopicsAndSubs(ctx context.Context) {
	for name, sub := range map[string]**pubsub.Subscription{
		topicHigh:   &subHigh,
		topicMedium: &subMedium,
		topicLow:    &subLow,
		topicImm:    &subImm,
	} {
		topic, err := psClient.CreateTopic(ctx, name)
		if err != nil {
			log.Fatalf("CreateTopic(%s): %v", name, err)
		}
		*sub, err = psClient.CreateSubscription(ctx, name+"-sub", pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 10 * time.Second,
		})
		if err != nil {
			log.Fatalf("CreateSubscription(%s-sub): %v", name, err)
		}
	}
}

func pullOne(ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) (*pubsub.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var msg *pubsub.Message
	err := sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		msg = m
		m.Ack()
		cancel()
	})
	if msg != nil {
		return msg, nil
	}
	if err != nil && err != context.Canceled {
		return nil, err
	}
	return nil, fmt.Errorf("no message received within %v", timeout)
}

func pullN(ctx context.Context, sub *pubsub.Subscription, n int, timeout time.Duration) ([]*pubsub.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var msgs []*pubsub.Message
	err := sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		msgs = append(msgs, m)
		m.Ack()
		if len(msgs) >= n {
			cancel()
		}
	})
	if len(msgs) >= n {
		return msgs[:n], nil
	}
	if err != nil && len(msgs) == 0 {
		return nil, err
	}
	return msgs, fmt.Errorf("got %d messages, wanted %d", len(msgs), n)
}

func drainSub(ctx context.Context, sub *pubsub.Subscription) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()
	})
}
