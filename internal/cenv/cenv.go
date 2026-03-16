// Package cenv provides environment variable access for the Jack service.
package cenv

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment variable names.
const (
	// Build info
	CommitSHA = "COMMIT_SHA"

	// Server configuration
	GRPCPort = "GRPC_PORT"

	// GCS configuration
	GCSBucket = "GCS_BUCKET"
	GCSPrefix = "GCS_PREFIX"

	// Queue configuration
	QueueBackend = "QUEUE_BACKEND" // "noop", "pubsub"

	// Datadog configuration
	DatadogStatsdAddr = "DATADOG_STATSD_ADDR"

	// Pub/Sub configuration
	PubSubProject        = "PUBSUB_PROJECT"
	PubSubTopicHigh      = "PUBSUB_TOPIC_HIGH"
	PubSubTopicMedium    = "PUBSUB_TOPIC_MEDIUM"
	PubSubTopicLow       = "PUBSUB_TOPIC_LOW"
	PubSubTopicImmediate = "PUBSUB_TOPIC_IMMEDIATE"

	// Cloud Tasks configuration (for scheduled/future jobs)
	CloudTasksProject        = "CLOUD_TASKS_PROJECT"
	CloudTasksLocation       = "CLOUD_TASKS_LOCATION"
	CloudTasksQueue          = "CLOUD_TASKS_QUEUE"
	CallbackBaseURL          = "CALLBACK_BASE_URL"
	CloudTasksServiceAccount = "CLOUD_TASKS_SERVICE_ACCOUNT"
	ScheduleThreshold        = "SCHEDULE_THRESHOLD" // duration string, default "1m"
)

// Get returns the value of an environment variable, trimmed of whitespace.
func Get(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

// GetOrDefault returns the value of an environment variable or a default if not set.
func GetOrDefault(key, defaultValue string) string {
	v := Get(key)
	if v == "" {
		return defaultValue
	}
	return v
}

// GetInt returns the integer value of an environment variable, or 0 if not set or invalid.
func GetInt(key string) int {
	v, _ := strconv.Atoi(Get(key))
	return v
}

// GetIntOrDefault returns the integer value of an environment variable or a default.
func GetIntOrDefault(key string, defaultValue int) int {
	v, err := strconv.Atoi(Get(key))
	if err != nil {
		return defaultValue
	}
	return v
}

// GetBool returns the boolean value of an environment variable, or false if not set or invalid.
func GetBool(key string) bool {
	v, _ := strconv.ParseBool(Get(key))
	return v
}

// GetBoolOrDefault returns the boolean value of an environment variable or a default.
func GetBoolOrDefault(key string, defaultValue bool) bool {
	v, err := strconv.ParseBool(Get(key))
	if err != nil {
		return defaultValue
	}
	return v
}

// GetDuration returns the duration value of an environment variable, or 0 if not set or invalid.
func GetDuration(key string) time.Duration {
	v, _ := time.ParseDuration(Get(key))
	return v
}

// GetDurationOrDefault returns the duration value of an environment variable or a default.
func GetDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	v, err := time.ParseDuration(Get(key))
	if err != nil {
		return defaultValue
	}
	return v
}

// IsSet returns true if the environment variable is set and non-empty.
func IsSet(key string) bool {
	return Get(key) != ""
}
