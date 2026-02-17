package queue

// MessageAdapter converts a queue.Job into the wire format expected by
// the downstream consumer. This isolates format concerns from the
// Pub/Sub backend, allowing different consumers to use different formats.
type MessageAdapter interface {
	// Marshal converts a Job into the byte payload for a Pub/Sub message.
	Marshal(job *Job) ([]byte, error)
}
