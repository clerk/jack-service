// Package main is the entry point for the Jack service.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/DataDog/datadog-go/v5/statsd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/clerk/jack-service/internal/api"
	"github.com/clerk/jack-service/internal/cenv"
	"github.com/clerk/jack-service/internal/config"
	"github.com/clerk/jack-service/internal/queue"
	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/proto/jackpb"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting Jack service...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize DogStatsD client
	var sd statsd.ClientInterface
	if cfg.DatadogStatsdAddr != "" {
		log.Printf("Connecting to DogStatsD at %s", cfg.DatadogStatsdAddr)
		sd, err = statsd.New(cfg.DatadogStatsdAddr, statsd.WithTags([]string{
			"service:jack-service",
			"env:" + cenv.GetOrDefault("ENV", "development"),
		}))
		if err != nil {
			log.Fatalf("Failed to initialize DogStatsD client: %v", err)
		}
	} else {
		sd = &statsd.NoOpClient{}
	}
	defer sd.Close()

	// Initialize storage
	var store storage.Store
	if cfg.GCSBucket != "" {
		log.Printf("Connecting to GCS bucket: %s", cfg.GCSBucket)
		gcsStore, err := storage.NewGCSStore(ctx, storage.GCSConfig{
			Bucket: cfg.GCSBucket,
			Prefix: cfg.GCSPrefix,
		})
		if err != nil {
			log.Fatalf("Failed to initialize storage: %v", err)
		}
		store = gcsStore
	} else {
		log.Println("Using in-memory storage (data will not persist)")
		store = storage.NewMemoryStore()
	}
	defer store.Close()

	// Initialize queue backend
	var backend queue.Backend
	switch cfg.QueueBackend {
	case "pubsub":
		log.Println("Using GCP Pub/Sub queue backend")
		pubsubBackend, err := queue.NewPubSubBackend(ctx, queue.PubSubConfig{
			Project:        cfg.PubSubProject,
			HighTopic:      cfg.PubSubTopicHigh,
			MediumTopic:    cfg.PubSubTopicMedium,
			LowTopic:       cfg.PubSubTopicLow,
			ImmediateTopic: cfg.PubSubTopicImmediate,
			Adapter:        &queue.LegacyAdapter{},
			Statsd:         sd,
		})
		if err != nil {
			log.Fatalf("Failed to initialize pubsub backend: %v", err)
		}
		backend = pubsubBackend
	case "noop":
		log.Println("Using noop queue backend (jobs will not be delivered)")
		backend = queue.NewNoopBackend(queue.DefaultConfig())
	default:
		log.Printf("Unknown queue backend: %s, using noop", cfg.QueueBackend)
		backend = queue.NewNoopBackend(queue.DefaultConfig())
	}
	defer backend.Close()

	// Start gRPC server
	grpcServer := grpc.NewServer()

	// Register runtime service (Enqueue, EnqueueBulk)
	runtimeServer := api.NewServer(store, backend, api.DefaultServerConfig(), sd)
	jackpb.RegisterBackgroundJobsServer(grpcServer, runtimeServer)

	// Register admin service (producer/job type CRUD)
	adminServer := api.NewAdminServer(store)
	jackpb.RegisterAdminServiceServer(grpcServer, adminServer)

	// Register gRPC health checking
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Enable reflection for debugging tools like grpcurl
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port: %v", err)
	}

	go func() {
		log.Printf("gRPC server listening on :%d", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// Graceful shutdown
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	grpcServer.GracefulStop()

	log.Println("Shutdown complete")
}
