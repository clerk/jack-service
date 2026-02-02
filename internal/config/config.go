// Package config provides configuration loading for the Jack service.
package config

import (
	"github.com/clerk/jack-service/internal/cenv"
)

// Config contains all configuration for the Jack service.
type Config struct {
	// Server configuration
	GRPCPort int
	HTTPPort int

	// GCS configuration
	GCSBucket string
	GCSPrefix string

	// Queue configuration
	QueueBackend string // "noop", "pubsub"

	// Pub/Sub configuration (if using pubsub backend)
	PubSubProject string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	return &Config{
		GRPCPort:      cenv.GetIntOrDefault(cenv.GRPCPort, 50051),
		HTTPPort:      cenv.GetIntOrDefault(cenv.HTTPPort, 8080),
		GCSBucket:     cenv.Get(cenv.GCSBucket),
		GCSPrefix:     cenv.Get(cenv.GCSPrefix),
		QueueBackend:  cenv.GetOrDefault(cenv.QueueBackend, "noop"),
		PubSubProject: cenv.Get(cenv.PubSubProject),
	}, nil
}
