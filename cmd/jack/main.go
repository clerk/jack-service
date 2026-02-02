// Package main is the entry point for the Jack service.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fullstorydev/grpcui/standalone"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"github.com/clerk/jack-service/internal/api"
	"github.com/clerk/jack-service/internal/config"
	"github.com/clerk/jack-service/internal/queue"
	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/internal/web"
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
	runtimeServer := api.NewServer(store, backend, api.DefaultServerConfig())
	jackpb.RegisterBackgroundJobsServer(grpcServer, runtimeServer)

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

	// Start HTTP server for health checks and web console
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Web console
	console := web.New(store)
	console.RegisterRoutes(httpMux)

	// gRPC UI (connect to our own gRPC server)
	go func() {
		// Wait a moment for gRPC server to be ready
		time.Sleep(100 * time.Millisecond)
		grpcConn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", cfg.GRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			log.Printf("Failed to connect to gRPC for UI: %v", err)
			return
		}
		grpcuiHandler, err := standalone.HandlerViaReflection(ctx, grpcConn, fmt.Sprintf("localhost:%d", cfg.GRPCPort))
		if err != nil {
			log.Printf("Failed to create gRPC UI handler: %v", err)
			return
		}
		httpMux.Handle("/grpc/", http.StripPrefix("/grpc", grpcuiHandler))
		log.Printf("gRPC UI available at http://localhost:%d/grpc/", cfg.HTTPPort)
	}()

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      httpMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("HTTP server listening on :%d (health checks)", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	grpcServer.GracefulStop()
	httpServer.Shutdown(shutdownCtx)

	log.Println("Shutdown complete")
}
