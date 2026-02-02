package storage

import (
	"context"
	"fmt"
	"io"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/clerk/jack-service/proto/jackpb"
)

const (
	producersFile = "producers.json"
	jobTypesFile  = "job_types.json"
)

// GCSStore implements Store using Google Cloud Storage.
type GCSStore struct {
	client *storage.Client
	bucket string
	prefix string

	mu        sync.RWMutex
	producers map[string]*jackpb.Producer
	jobTypes  map[string]*jackpb.JobType
}

// GCSConfig contains configuration for the GCS store.
type GCSConfig struct {
	Bucket string
	Prefix string
}

// NewGCSStore creates a new GCS-backed store.
func NewGCSStore(ctx context.Context, config GCSConfig) (*GCSStore, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	store := &GCSStore{
		client:    client,
		bucket:    config.Bucket,
		prefix:    config.Prefix,
		producers: make(map[string]*jackpb.Producer),
		jobTypes:  make(map[string]*jackpb.JobType),
	}

	if err := store.load(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to load data: %w", err)
	}

	return store, nil
}

func (s *GCSStore) objectPath(name string) string {
	if s.prefix == "" {
		return name
	}
	return s.prefix + "/" + name
}

func (s *GCSStore) load(ctx context.Context) error {
	bucket := s.client.Bucket(s.bucket)

	prods, err := s.loadProducers(ctx, bucket)
	if err != nil {
		return err
	}

	types, err := s.loadJobTypes(ctx, bucket)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.producers = make(map[string]*jackpb.Producer)
	for _, prod := range prods {
		s.producers[prod.ProducerId] = prod
	}

	s.jobTypes = make(map[string]*jackpb.JobType)
	for _, jt := range types {
		key := jobTypeKey(jt.ProducerId, jt.Name)
		s.jobTypes[key] = jt
	}

	return nil
}

func (s *GCSStore) loadProducers(ctx context.Context, bucket *storage.BucketHandle) ([]*jackpb.Producer, error) {
	obj := bucket.Object(s.objectPath(producersFile))
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read producers: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read producers data: %w", err)
	}

	var file jackpb.ProducersFile
	if err := protojson.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("failed to parse producers: %w", err)
	}

	return file.Producers, nil
}

func (s *GCSStore) loadJobTypes(ctx context.Context, bucket *storage.BucketHandle) ([]*jackpb.JobType, error) {
	obj := bucket.Object(s.objectPath(jobTypesFile))
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read job types: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read job types data: %w", err)
	}

	var file jackpb.JobTypesFile
	if err := protojson.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("failed to parse job types: %w", err)
	}

	return file.JobTypes, nil
}

func (s *GCSStore) saveProducers(ctx context.Context) error {
	prods := make([]*jackpb.Producer, 0, len(s.producers))
	for _, prod := range s.producers {
		prods = append(prods, prod)
	}

	file := &jackpb.ProducersFile{Producers: prods}
	data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(file)
	if err != nil {
		return fmt.Errorf("failed to marshal producers: %w", err)
	}

	obj := s.client.Bucket(s.bucket).Object(s.objectPath(producersFile))
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write producers: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close producers writer: %w", err)
	}

	return nil
}

func (s *GCSStore) saveJobTypes(ctx context.Context) error {
	types := make([]*jackpb.JobType, 0, len(s.jobTypes))
	for _, jt := range s.jobTypes {
		types = append(types, jt)
	}

	file := &jackpb.JobTypesFile{JobTypes: types}
	data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(file)
	if err != nil {
		return fmt.Errorf("failed to marshal job types: %w", err)
	}

	obj := s.client.Bucket(s.bucket).Object(s.objectPath(jobTypesFile))
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write job types: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close job types writer: %w", err)
	}

	return nil
}

// CreateProducer creates a new producer.
func (s *GCSStore) CreateProducer(ctx context.Context, producer *jackpb.Producer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.producers[producer.ProducerId]; exists {
		return fmt.Errorf("%w: producer with ID %s", ErrAlreadyExists, producer.ProducerId)
	}

	now := timestamppb.Now()
	producer.CreatedAt = now
	producer.UpdatedAt = now

	prodCopy := proto.Clone(producer).(*jackpb.Producer)
	s.producers[producer.ProducerId] = prodCopy

	if err := s.saveProducers(ctx); err != nil {
		delete(s.producers, producer.ProducerId)
		return err
	}

	return nil
}

// GetProducer retrieves a producer by ID.
func (s *GCSStore) GetProducer(ctx context.Context, producerID string) (*jackpb.Producer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prod, exists := s.producers[producerID]
	if !exists {
		return nil, fmt.Errorf("%w: producer %s", ErrNotFound, producerID)
	}

	return proto.Clone(prod).(*jackpb.Producer), nil
}

// ListProducers returns all producers.
func (s *GCSStore) ListProducers(ctx context.Context) ([]*jackpb.Producer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prods := make([]*jackpb.Producer, 0, len(s.producers))
	for _, prod := range s.producers {
		prods = append(prods, proto.Clone(prod).(*jackpb.Producer))
	}
	return prods, nil
}

// CreateJobType creates a new job type.
func (s *GCSStore) CreateJobType(ctx context.Context, jobType *jackpb.JobType) error {
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

	jtCopy := proto.Clone(jobType).(*jackpb.JobType)
	s.jobTypes[key] = jtCopy

	if err := s.saveJobTypes(ctx); err != nil {
		delete(s.jobTypes, key)
		return err
	}

	return nil
}

// GetJobType retrieves a job type.
func (s *GCSStore) GetJobType(ctx context.Context, producerID, jobTypeName string) (*jackpb.JobType, error) {
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
func (s *GCSStore) ListJobTypes(ctx context.Context, producerID string) ([]*jackpb.JobType, error) {
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

// Close releases resources.
func (s *GCSStore) Close() error {
	return s.client.Close()
}

var _ Store = (*GCSStore)(nil)
