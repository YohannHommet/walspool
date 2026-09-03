package walspool

import (
	"context"
	"time"
)

// Spooler is the Driving (Inbound) Port.
// High-leverage, small surface: callers only interact with this interface.
type Spooler interface {
	// Enqueue accepts a topic and raw payload into durable storage.
	// Fails fast with ErrPreconditionViolated if parameters are invalid,
	// ErrSpoolFull if capacity is exhausted under backpressure,
	// or ErrSpoolerClosed if already shut down.
	Enqueue(ctx context.Context, topic string, payload []byte) error

	// Flush blocks until all currently committed records in the spool are drained.
	Flush(ctx context.Context) error

	// Close gracefully terminates the background dispatcher and releases storage handles.
	Close() error
}

// Sink is the Driven (Outbound) Port for downstream external delivery.
// Infrastructure adapters (HTTP webhooks, Kafka publishers, S3 uploaders) satisfy this.
type Sink interface {
	// Deliver transmits a batch of records.
	// Return nil on success.
	// Return ErrPermanentRejection to bypass retries and dead-letter the batch.
	// Return ErrSinkUnavailable to trigger exponential backoff.
	Deliver(ctx context.Context, batch []Record) error
}

// StorageEngine is the Driven (Outbound) Port for write-ahead log persistence.
type StorageEngine interface {
	// Append atomically commits a record to the log.
	Append(rec Record) (Offset, error)

	// ReadBatch reads up to maxCount uncheckpointed records.
	ReadBatch(maxCount int) ([]Record, error)

	// Commit checkpoints all records up to and including the specified offset.
	Commit(offset Offset) error

	// UncommittedCount returns the number of pending records awaiting delivery.
	UncommittedCount() (int, error)

	// Close flushes buffers and closes underlying file descriptors.
	Close() error
}

// Clock is a test seam allowing sub-second, deterministic time manipulation.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// RealClock is the production Clock adapter using standard runtime time.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
