package walspool

import "fmt"

// MaxTopicBytes is the maximum topic length representable by the WAL wire format.
// It is shared by the engine, storage adapters, and ingress adapters so the same
// record is accepted or rejected identically everywhere.
const MaxTopicBytes = 65535

// Record validation sentinels. Each wraps ErrPreconditionViolated so existing
// callers that classify client faults continue to work, while the new sentinels
// let ingress adapters map topic/payload faults precisely.
var (
	ErrEmptyTopic      = fmt.Errorf("%w: topic must not be empty", ErrPreconditionViolated)
	ErrTopicTooLong    = fmt.Errorf("%w: topic length exceeds %d bytes", ErrPreconditionViolated, MaxTopicBytes)
	ErrEmptyPayload    = fmt.Errorf("%w: payload must not be empty", ErrPreconditionViolated)
	ErrPayloadTooLarge = fmt.Errorf("%w: payload size exceeds the configured limit", ErrPreconditionViolated)
)

// ValidateTopic checks the wire-level topic bound shared by all storage adapters.
func ValidateTopic(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if len(topic) > MaxTopicBytes {
		return fmt.Errorf("%w: got %d bytes", ErrTopicTooLong, len(topic))
	}
	return nil
}

// ValidatePayload checks non-empty payload and the configured maximum size.
// A non-positive max disables only the upper bound.
func ValidatePayload(payload []byte, maxPayload int) error {
	if len(payload) == 0 {
		return ErrEmptyPayload
	}
	if maxPayload > 0 && len(payload) > maxPayload {
		return fmt.Errorf("%w: got %d bytes, limit %d", ErrPayloadTooLarge, len(payload), maxPayload)
	}
	return nil
}

// ValidateRecord applies the complete client-fault validation shared by Engine
// and storage adapters.
func ValidateRecord(topic string, payload []byte, maxPayload int) error {
	if err := ValidateTopic(topic); err != nil {
		return err
	}
	return ValidatePayload(payload, maxPayload)
}
