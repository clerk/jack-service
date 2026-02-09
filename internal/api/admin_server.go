package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/clerk/jack-service/internal/storage"
	"github.com/clerk/jack-service/proto/jackpb"
)

// AdminServer implements the AdminService gRPC service.
type AdminServer struct {
	jackpb.UnimplementedAdminServiceServer

	store storage.Store
}

// NewAdminServer creates a new AdminServer.
func NewAdminServer(store storage.Store) *AdminServer {
	return &AdminServer{store: store}
}

func generateProducerID() string {
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("prod_%s", hex.EncodeToString(b))
}

// CreateProducer registers a new producer with a server-generated ID.
func (s *AdminServer) CreateProducer(ctx context.Context, req *jackpb.CreateProducerRequest) (*jackpb.CreateProducerResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	producer := &jackpb.Producer{
		ProducerId:   generateProducerID(),
		Name:         req.Name,
		Description:  req.Description,
		RateLimitRps: 1000,
	}

	if err := s.store.CreateProducer(ctx, producer); err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Error(codes.Internal, "failed to create producer")
	}

	return &jackpb.CreateProducerResponse{Producer: producer}, nil
}

// GetProducer retrieves a single producer by ID.
func (s *AdminServer) GetProducer(ctx context.Context, req *jackpb.GetProducerRequest) (*jackpb.GetProducerResponse, error) {
	if req.ProducerId == "" {
		return nil, status.Error(codes.InvalidArgument, "producer_id is required")
	}

	producer, err := s.store.GetProducer(ctx, req.ProducerId)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "producer not found: %s", req.ProducerId)
		}
		return nil, status.Error(codes.Internal, "failed to get producer")
	}

	return &jackpb.GetProducerResponse{Producer: producer}, nil
}

// ListProducers returns all registered producers.
func (s *AdminServer) ListProducers(ctx context.Context, req *jackpb.ListProducersRequest) (*jackpb.ListProducersResponse, error) {
	producers, err := s.store.ListProducers(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list producers")
	}

	return &jackpb.ListProducersResponse{Producers: producers}, nil
}

// CreateJobType registers a new job type for a producer.
func (s *AdminServer) CreateJobType(ctx context.Context, req *jackpb.CreateJobTypeRequest) (*jackpb.CreateJobTypeResponse, error) {
	if req.ProducerId == "" {
		return nil, status.Error(codes.InvalidArgument, "producer_id is required")
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	maxRetries := req.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	jobType := &jackpb.JobType{
		ProducerId:  req.ProducerId,
		Name:        req.Name,
		Queue:       req.Queue,
		MaxRetries:  maxRetries,
		Description: req.Description,
	}

	if err := s.store.CreateJobType(ctx, jobType); err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "producer not found: %s", req.ProducerId)
		}
		return nil, status.Error(codes.Internal, "failed to create job type")
	}

	return &jackpb.CreateJobTypeResponse{JobType: jobType}, nil
}

// ListJobTypes returns all job types for a given producer.
func (s *AdminServer) ListJobTypes(ctx context.Context, req *jackpb.ListJobTypesRequest) (*jackpb.ListJobTypesResponse, error) {
	if req.ProducerId == "" {
		return nil, status.Error(codes.InvalidArgument, "producer_id is required")
	}

	jobTypes, err := s.store.ListJobTypes(ctx, req.ProducerId)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list job types")
	}

	return &jackpb.ListJobTypesResponse{JobTypes: jobTypes}, nil
}
