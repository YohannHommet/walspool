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
	BatchSize      int           // Number of records per batch drain.
	FlushInterval  time.Duration // Maximum wait time before dispatching an incomplete batch.
	InitialBackoff time.Duration // Initial retry delay upon transient sink errors.
	MaxBackoff     time.Duration // Maximum retry delay cap for exponential backoff.
	MaxPayloadSize int           // Maximum allowed record payload size in bytes (Meyer DbC precondition).
	MaxCapacity    int           // Default backpressure quota (maximum pending uncommitted records allowed in storage).
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

// Option configures an Engine instance.
type Option func(*Engine)

// WithObserver registers an IngestionObserver to be notified on successful record ingestion.
func WithObserver(obs IngestionObserver) Option {
	return func(e *Engine) {
		if obs != nil {
			e.observers = append(e.observers, obs)
		}
	}
}

type flushRequest struct {
	ctx context.Context
	ack chan error
}

// Engine implements the Spooler interface.
// High leverage: encapsulates WAL coordination, batching, backpressure, and exponential backoff.
type Engine struct {
	cfg       Config
	storage   StorageEngine
	sink      Sink
	clock     Clock
	observers []IngestionObserver

	idCounter uint64
	notifyCh  chan struct{}
	flushCh   chan flushRequest
	stopCh    chan struct{}
	doneWg    sync.WaitGroup

	mu     sync.RWMutex
	closed bool
}

// New creates and starts a Spooler with injected dependencies and functional options.
func New(cfg Config, storage StorageEngine, sink Sink, clock Clock, opts ...Option) (*Engine, error) {
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
		cfg:       cfg,
		storage:   storage,
		sink:      sink,
		clock:     clock,
		notifyCh:  make(chan struct{}, 1),
		flushCh:   make(chan flushRequest),
		stopCh:    make(chan struct{}),
		observers: make([]IngestionObserver, 0),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	e.doneWg.Add(1)
	go e.runDispatcher()

	return e, nil
}

func (e *Engine) Enqueue(ctx context.Context, topic string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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
	defer e.mu.RUnlock()
	if e.closed {
		return ErrSpoolerClosed
	}

	// Defensive copy: insulate internal storage from caller mutating slice post-enqueue
	payloadCopy := append([]byte(nil), payload...)

	rec := Record{
		ID:        atomic.AddUint64(&e.idCounter, 1),
		Timestamp: e.clock.Now(),
		Topic:     topic,
		Payload:   payloadCopy,
	}

	offset, err := e.storage.Append(rec)
	if err != nil {
		// Tier 2 (ErrSpoolFull) or Tier 3 (ErrStorageUnavailable)
		return err
	}
	rec.Offset = offset

	for _, obs := range e.observers {
		obs.OnIngested(rec)
	}

	// Wake up dispatcher non-blockingly
	select {
	case e.notifyCh <- struct{}{}:
	default:
	}

	return nil
}

func (e *Engine) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.RLock()
	closed := e.closed
	e.mu.RUnlock()
	if closed {
		return ErrSpoolerClosed
	}

	ack := make(chan error, 1)
	req := flushRequest{ctx: ctx, ack: ack}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.stopCh:
		return ErrSpoolerClosed
	case e.flushCh <- req:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.stopCh:
		return ErrSpoolerClosed
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
		case req := <-e.flushCh:
			req.ack <- e.drainAll(req.ctx)
			continue
		default:
		}

		// If records were drained, immediately check for more before sleeping
		if drained {
			continue
		}

		select {
		case <-e.stopCh:
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = e.drainAll(shutdownCtx)
			cancel()
			return
		case req := <-e.flushCh:
			req.ack <- e.drainAll(req.ctx)
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

func (e *Engine) drainAll(ctx context.Context) error {
	currentBackoff := e.cfg.InitialBackoff
	for {
		drained, err := e.drainPendingBatches()
		if err != nil {
			if IsTransient(err) {
				select {
				case <-e.stopCh:
					return ErrSpoolerClosed
				case <-ctx.Done():
					return ctx.Err()
				case <-e.clock.After(currentBackoff):
					currentBackoff = e.calculateNextBackoff(currentBackoff)
					continue
				}
			}
			return err
		}
		if !drained {
			return nil
		}
		currentBackoff = e.cfg.InitialBackoff
	}
}
