package walspool

import (
	"errors"
	"fmt"
)

// Tier 1: Contract Violations / Preconditions (Caller Fault)
var (
	// ErrPreconditionViolated occurs when a caller provides an invalid topic,
	// empty payload, or violates invariant constraints.
	ErrPreconditionViolated = errors.New("walspool: precondition violated")

	// ErrSpoolerClosed indicates an operation was attempted on a terminated spooler.
	ErrSpoolerClosed = errors.New("walspool: spooler is closed")

	// ErrHubClosed indicates an operation was attempted on a terminated log hub.
	ErrHubClosed = fmt.Errorf("%w: log hub is closed", ErrSpoolerClosed)

	// ErrCorruptRecord indicates a record failed magic byte validation or CRC32 checksum verification.
	ErrCorruptRecord = errors.New("walspool: corrupt record or checksum mismatch")

	// ErrTruncatedData indicates unexpected EOF or partial record encountered during deserialization.
	ErrTruncatedData = errors.New("walspool: unexpected EOF or truncated data")
)

// Tier 2: Expected Domain Outcomes (Operational States)
var (
	// ErrSpoolFull occurs when backpressure rejects a write because storage quota is exhausted.
	ErrSpoolFull = errors.New("walspool: spool capacity exhausted")

	// ErrPermanentRejection indicates a sink explicitly rejected a batch as unprocessable.
	// The batch is moved to the dead-letter queue without retry.
	ErrPermanentRejection = errors.New("walspool: sink permanently rejected payload")
)

// Tier 3: Infrastructure / System Faults (Translated at Boundary)
var (
	// ErrStorageUnavailable indicates an unrecoverable disk I/O failure or corruption.
	ErrStorageUnavailable = errors.New("walspool: storage engine failure")

	// ErrSinkUnavailable indicates transient network, timeout, or downstream unavailability.
	// Triggers exponential backoff retry in the background dispatcher.
	ErrSinkUnavailable = errors.New("walspool: remote sink unavailable")
)

// IsTransient returns true if an error warrants retry with backoff.
func IsTransient(err error) bool {
	return errors.Is(err, ErrSinkUnavailable)
}
