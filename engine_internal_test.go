package walspool

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestCalculateNextBackoff_DecorrelatedJitter(t *testing.T) {
	cfg := Config{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
	}
	e := &Engine{cfg: cfg}

	const iterations = 10
	seq1 := make([]time.Duration, iterations)
	seq2 := make([]time.Duration, iterations)

	curr1 := cfg.InitialBackoff
	for i := 0; i < iterations; i++ {
		curr1 = e.calculateNextBackoff(curr1)
		if curr1 < cfg.InitialBackoff || curr1 > cfg.MaxBackoff {
			t.Fatalf("seq1[%d] = %v out of bounds [%v, %v]", i, curr1, cfg.InitialBackoff, cfg.MaxBackoff)
		}
		seq1[i] = curr1
	}

	curr2 := cfg.InitialBackoff
	for i := 0; i < iterations; i++ {
		curr2 = e.calculateNextBackoff(curr2)
		if curr2 < cfg.InitialBackoff || curr2 > cfg.MaxBackoff {
			t.Fatalf("seq2[%d] = %v out of bounds [%v, %v]", i, curr2, cfg.InitialBackoff, cfg.MaxBackoff)
		}
		seq2[i] = curr2
	}

	// Decorrelated jitter must produce distinct stochastic sequences across runs
	if slices.Equal(seq1, seq2) {
		t.Fatalf("expected non-deterministic sequences due to decorrelated jitter, but both runs produced identical sequences: %v", seq1)
	}
}

func TestCalculateNextBackoff_ZeroAlloc(t *testing.T) {
	cfg := Config{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
	}
	e := &Engine{cfg: cfg}

	allocs := testing.AllocsPerRun(1000, func() {
		_ = e.calculateNextBackoff(100 * time.Millisecond)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for calculateNextBackoff, got %f", allocs)
	}
}

func TestCalculateNextBackoff_DefensiveBounds(t *testing.T) {
	t.Run("zero or negative initial backoff falls back to default", func(t *testing.T) {
		e := &Engine{cfg: Config{InitialBackoff: 0, MaxBackoff: 500 * time.Millisecond}}
		b := e.calculateNextBackoff(0)
		if b < DefaultConfig().InitialBackoff {
			t.Fatalf("expected at least default initial backoff (%v), got %v", DefaultConfig().InitialBackoff, b)
		}
	})

	t.Run("max backoff smaller than initial backoff", func(t *testing.T) {
		e := &Engine{cfg: Config{InitialBackoff: 100 * time.Millisecond, MaxBackoff: 10 * time.Millisecond}}
		b := e.calculateNextBackoff(100 * time.Millisecond)
		if b < 100*time.Millisecond {
			t.Fatalf("expected backoff >= initial backoff (100ms), got %v", b)
		}
	})

	t.Run("current smaller than initial backoff", func(t *testing.T) {
		e := &Engine{cfg: Config{InitialBackoff: 50 * time.Millisecond, MaxBackoff: 500 * time.Millisecond}}
		b := e.calculateNextBackoff(5 * time.Millisecond)
		if b < 50*time.Millisecond {
			t.Fatalf("expected backoff >= initial backoff (50ms), got %v", b)
		}
	})
}

func TestResetTimer_ZeroAlloc(t *testing.T) {
	tm := time.NewTimer(time.Hour)
	defer tm.Stop()

	allocs := testing.AllocsPerRun(1000, func() {
		resetTimer(tm, time.Hour)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 allocs/op for resetTimer, got %f", allocs)
	}
}

func TestResetTimer_DrainsPendingTick(t *testing.T) {
	tm := time.NewTimer(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	resetTimer(tm, time.Hour)
	defer tm.Stop()

	select {
	case <-tm.C:
		t.Fatal("timer channel should have been drained by resetTimer")
	default:
	}
}

type mockFailingSink struct {
	err error
}

func (m *mockFailingSink) Deliver(ctx context.Context, batch []Record) error {
	return m.err
}

func TestDrainAll_ContextCancellationDuringBackoff(t *testing.T) {
	storage := NewMemoryStorageEngine(10)
	_, _ = storage.Append(Record{ID: 1, Topic: "test", Payload: []byte("val")})
	e := &Engine{
		cfg: Config{
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     10 * time.Second,
			BatchSize:      10,
		},
		storage: storage,
		sink:    &mockFailingSink{err: ErrSinkUnavailable},
		stopCh:  make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := e.drainAll(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got: %v", err)
	}
	if elapsed >= 1*time.Second {
		t.Fatalf("drainAll took %v, expected abort in ~50ms", elapsed)
	}
}

func TestDrainAll_ImmediateStop(t *testing.T) {
	storage := NewMemoryStorageEngine(10)
	_, _ = storage.Append(Record{ID: 1, Topic: "test", Payload: []byte("val")})
	stopCh := make(chan struct{})
	e := &Engine{
		cfg: Config{
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     10 * time.Second,
			BatchSize:      10,
		},
		storage: storage,
		sink:    &mockFailingSink{err: ErrSinkUnavailable},
		stopCh:  stopCh,
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		close(stopCh)
	}()

	start := time.Now()
	err := e.drainAll(context.Background())
	elapsed := time.Since(start)

	if !errors.Is(err, ErrSpoolerClosed) {
		t.Fatalf("expected ErrSpoolerClosed, got: %v", err)
	}
	if elapsed >= 1*time.Second {
		t.Fatalf("drainAll took %v, expected immediate stop (< 1s)", elapsed)
	}
}

func BenchmarkCalculateNextBackoff(b *testing.B) {
	e := &Engine{cfg: DefaultConfig()}
	curr := e.cfg.InitialBackoff
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		curr = e.calculateNextBackoff(curr)
	}
}

func BenchmarkTimerReset(b *testing.B) {
	tm := time.NewTimer(time.Hour)
	defer tm.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resetTimer(tm, time.Hour)
	}
}
