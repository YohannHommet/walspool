package walspool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds configuration parameters for the Spooler.
type Config struct {
	BatchSize      int
	FlushInterval  time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxPayloadSize int
	MaxCapacity    int
}

// DefaultConfig provides production-safe baseline defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize:      100,
		FlushInterval:  50 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		MaxPayloadSize: 1024 * 1024, // 1MB
		MaxCapacity:    10000,
	}
}

// Engine implements the Spooler interface.
// High leverage: encapsulates WAL coordination, batching, backpressure, and exponential backoff.
type Engine struct {
	cfg     Config
	storage StorageEngine
	sink    Sink
	clock   Clock

	idCounter uint64
	notifyCh  chan struct{}
	flushCh   chan chan error
	stopCh    chan struct{}
	doneWg    sync.WaitGroup

	mu     sync.RWMutex
	closed bool
}

// New creates and starts a Spooler with injected dependencies.
func New(cfg Config, storage StorageEngine, sink Sink, clock Clock) (*Engine, error) {
	if storage == nil || sink == nil {
		return nil, fmt.Errorf("%w: storage and sink dependencies must not be nil", ErrPreconditionViolated)
	}
	if clock == nil {
		clock = RealClock{}
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 50 * time.Millisecond
	}
	if cfg.MaxPayloadSize <= 0 {
		cfg.MaxPayloadSize = 1024 * 1024
	}

	e := &Engine{
		cfg:      cfg,
		storage:  storage,
		sink:     sink,
		clock:    clock,
		notifyCh: make(chan struct{}, 1),
		flushCh:  make(chan chan error),
		stopCh:   make(chan struct{}),
	}

	e.doneWg.Add(1)
	go e.runDispatcher()

	return e, nil
}

func (e *Engine) Enqueue(ctx context.Context, topic string, payload []byte) error {
	// Precondition Verification (Meyer DbC - Client Fault)
	if topic == "" {
		return fmt.Errorf("%w: topic must not be empty", ErrPreconditionViolated)
	}
	if len(payload) == 0 {
		return fmt.Errorf("%w: payload must not be empty", ErrPreconditionViolated)
	}
	if len(payload) > e.cfg.MaxPayloadSize {
		return fmt.Errorf("%w: payload size %d exceeds limit of %d", ErrPreconditionViolated, len(payload), e.cfg.MaxPayloadSize)
	}

	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return ErrSpoolerClosed
	}

	rec := Record{
		ID:        atomic.AddUint64(&e.idCounter, 1),
		Timestamp: e.clock.Now(),
		Topic:     topic,
		Payload:   payload,
	}

	if _, err := e.storage.Append(rec); err != nil {
		// Tier 2 (ErrSpoolFull) or Tier 3 (ErrStorageUnavailable)
		return err
	}

	// Wake up dispatcher non-blockingly
	select {
	case e.notifyCh <- struct{}{}:
	default:
	}

	return nil
}

func (e *Engine) Flush(ctx context.Context) error {
	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return ErrSpoolerClosed
	}

	ack := make(chan error, 1)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.flushCh <- ack:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ack:
		return err
	}
}

func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.stopCh)
	e.doneWg.Wait()

	return e.storage.Close()
}

func (e *Engine) runDispatcher() {
	defer e.doneWg.Done()

	currentBackoff := e.cfg.InitialBackoff
	for {
		// Drain all available records up to BatchSize
		drained, err := e.drainPendingBatches()
		if err != nil && IsTransient(err) {
			// Apply exponential backoff when sink is experiencing transient faults
			select {
			case <-e.stopCh:
				return
			case <-e.clock.After(currentBackoff):
				currentBackoff = e.calculateNextBackoff(currentBackoff)
				continue
			}
		}

		// Reset backoff on successful delivery
		currentBackoff = e.cfg.InitialBackoff

		// Handle flush requests if all pending records are drained
		select {
		case ack := <-e.flushCh:
			ack <- e.drainAll()
			continue
		default:
		}

		// If records were drained, immediately check for more before sleeping
		if drained {
			continue
		}

		select {
		case <-e.stopCh:
			_ = e.drainAll()
			return
		case ack := <-e.flushCh:
			ack <- e.drainAll()
		case <-e.notifyCh:
		case <-e.clock.After(e.cfg.FlushInterval):
		}
	}
}

func (e *Engine) drainPendingBatches() (bool, error) {
	batch, err := e.storage.ReadBatch(e.cfg.BatchSize)
	if err != nil {
		return false, err
	}
	if len(batch) == 0 {
		return false, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deliverErr := e.sink.Deliver(ctx, batch)
	if deliverErr != nil {
		if errors.Is(deliverErr, ErrPermanentRejection) {
			// Sink permanently rejected; commit to advance past poisonous batch
			lastOffset := batch[len(batch)-1].Offset
			_ = e.storage.Commit(lastOffset)
			return true, nil
		}
		// Transient sink error; do not commit, will retry
		return false, deliverErr
	}

	lastOffset := batch[len(batch)-1].Offset
	if err := e.storage.Commit(lastOffset); err != nil {
		return false, err
	}

	return true, nil
}

func (e *Engine) calculateNextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > e.cfg.MaxBackoff {
		return e.cfg.MaxBackoff
	}
	return next
}

func (e *Engine) drainAll() error {
	for {
		drained, err := e.drainPendingBatches()
		if err != nil {
			return err
		}
		if !drained {
			return nil
		}
	}
}
