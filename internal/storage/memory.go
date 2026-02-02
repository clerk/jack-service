package storage

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/clerk/jack-service/proto/jackpb"
)

// MemoryStore implements Store using in-memory maps.
type MemoryStore struct {
	mu        sync.RWMutex
	producers map[string]*jackpb.Producer
	jobTypes  map[string]*jackpb.JobType
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		producers: make(map[string]*jackpb.Producer),
		jobTypes:  make(map[string]*jackpb.JobType),
	}
}

// CreateProducer creates a new producer.
func (s *MemoryStore) CreateProducer(ctx context.Context, producer *jackpb.Producer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.producers[producer.ProducerId]; exists {
		return fmt.Errorf("%w: producer with ID %s", ErrAlreadyExists, producer.ProducerId)
	}

	now := timestamppb.Now()
	producer.CreatedAt = now
	producer.UpdatedAt = now

	s.producers[producer.ProducerId] = proto.Clone(producer).(*jackpb.Producer)
	return nil
}

// GetProducer retrieves a producer by ID.
func (s *MemoryStore) GetProducer(ctx context.Context, producerID string) (*jackpb.Producer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prod, exists := s.producers[producerID]
	if !exists {
		return nil, fmt.Errorf("%w: producer %s", ErrNotFound, producerID)
	}

	return proto.Clone(prod).(*jackpb.Producer), nil
}

// ListProducers returns all producers.
func (s *MemoryStore) ListProducers(ctx context.Context) ([]*jackpb.Producer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prods := make([]*jackpb.Producer, 0, len(s.producers))
	for _, prod := range s.producers {
		prods = append(prods, proto.Clone(prod).(*jackpb.Producer))
	}
	return prods, nil
}

// CreateJobType creates a new job type.
func (s *MemoryStore) CreateJobType(ctx context.Context, jobType *jackpb.JobType) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.producers[jobType.ProducerId]; !exists {
		return fmt.Errorf("%w: producer %s", ErrNotFound, jobType.ProducerId)
	}

	key := jobTypeKey(jobType.ProducerId, jobType.Name)
	if _, exists := s.jobTypes[key]; exists {
		return fmt.Errorf("%w: job type %s for producer %s", ErrAlreadyExists, jobType.Name, jobType.ProducerId)
	}

	now := timestamppb.Now()
	jobType.CreatedAt = now
	jobType.UpdatedAt = now

	s.jobTypes[key] = proto.Clone(jobType).(*jackpb.JobType)
	return nil
}

// GetJobType retrieves a job type.
func (s *MemoryStore) GetJobType(ctx context.Context, producerID, jobTypeName string) (*jackpb.JobType, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := jobTypeKey(producerID, jobTypeName)
	jt, exists := s.jobTypes[key]
	if !exists {
		return nil, fmt.Errorf("%w: job type %s for producer %s", ErrNotFound, jobTypeName, producerID)
	}

	return proto.Clone(jt).(*jackpb.JobType), nil
}

// ListJobTypes returns all job types for a producer.
func (s *MemoryStore) ListJobTypes(ctx context.Context, producerID string) ([]*jackpb.JobType, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	types := make([]*jackpb.JobType, 0)
	for _, jt := range s.jobTypes {
		if jt.ProducerId == producerID {
			types = append(types, proto.Clone(jt).(*jackpb.JobType))
		}
	}
	return types, nil
}

// Close is a no-op.
func (s *MemoryStore) Close() error {
	return nil
}

var _ Store = (*MemoryStore)(nil)
