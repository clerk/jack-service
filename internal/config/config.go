// Package config provides configuration loading for the Jack service.
package config

import (
	"github.com/clerk/jack-service/internal/cenv"
)

// Config contains all configuration for the Jack service.
type Config struct {
	// Server configuration
	GRPCPort int

	// Datadog configuration
	DatadogStatsdAddr string

	// GCS configuration
	GCSBucket string
	GCSPrefix string

	// Queue configuration
	QueueBackend string // "noop", "pubsub"

	// Pub/Sub configuration (if using pubsub backend)
	PubSubProject        string
	PubSubTopicHigh      string
	PubSubTopicMedium    string
	PubSubTopicLow       string
	PubSubTopicImmediate string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	return &Config{
		DatadogStatsdAddr:    cenv.Get(cenv.DatadogStatsdAddr),
		GRPCPort:             cenv.GetIntOrDefault(cenv.GRPCPort, 50051),
		GCSBucket:            cenv.Get(cenv.GCSBucket),
		GCSPrefix:            cenv.Get(cenv.GCSPrefix),
		QueueBackend:         cenv.GetOrDefault(cenv.QueueBackend, "noop"),
		PubSubProject:        cenv.Get(cenv.PubSubProject),
		PubSubTopicHigh:      cenv.Get(cenv.PubSubTopicHigh),
		PubSubTopicMedium:    cenv.Get(cenv.PubSubTopicMedium),
		PubSubTopicLow:       cenv.Get(cenv.PubSubTopicLow),
		PubSubTopicImmediate: cenv.Get(cenv.PubSubTopicImmediate),
	}, nil
}
