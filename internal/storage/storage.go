package storage

import (
	"context"
	"errors"

	"github.com/clerk/jack-service/proto/jackpb"
)

// Common errors returned by storage implementations.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

func jobTypeKey(producerID, jobTypeName string) string {
	return producerID + ":" + jobTypeName
}

// Store defines the interface for configuration storage.
type Store interface {
	// Producer operations
	CreateProducer(ctx context.Context, producer *jackpb.Producer) error
	GetProducer(ctx context.Context, producerID string) (*jackpb.Producer, error)
	ListProducers(ctx context.Context) ([]*jackpb.Producer, error)

	// JobType operations
	CreateJobType(ctx context.Context, jobType *jackpb.JobType) error
	GetJobType(ctx context.Context, producerID, jobTypeName string) (*jackpb.JobType, error)
	ListJobTypes(ctx context.Context, producerID string) ([]*jackpb.JobType, error)

	// Close releases any resources.
	Close() error
}
