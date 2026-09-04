package walspool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	magicByte1 byte = 0x57 // 'W'
	magicByte2 byte = 0x53 // 'S'
	wireVersion byte = 0x01
	headerSize       = 29 // 2 (magic) + 1 (ver) + 4 (crc) + 8 (id) + 8 (time) + 2 (topicLen) + 4 (payloadLen)
)

var (
	ErrCorruptRecord = errors.New("walspool: corrupt record or checksum mismatch")
	ErrTruncatedData = errors.New("walspool: unexpected EOF or truncated data")
)

// Offset represents a monotonic position in the write-ahead log.
type Offset uint64

// Record is an immutable, durable unit of work in the spooler.
type Record struct {
	Offset    Offset
	ID        uint64
	Timestamp time.Time
	Topic     string
	Payload   []byte
	Checksum  uint32
}

// MarshalBinary serializes the record into an append-only binary chunk with CRC32 integrity.
// Header Layout: [Magic:2][Ver:1][CRC32:4][ID:8][UnixNano:8][TopicLen:2][PayloadLen:4][Topic...][Payload...]
func (r Record) MarshalBinary() ([]byte, error) {
	topicBytes := []byte(r.Topic)
	if len(topicBytes) > 65535 {
		return nil, fmt.Errorf("%w: topic length exceeds 64KB", ErrPreconditionViolated)
	}

	bodyLen := len(topicBytes) + len(r.Payload)
	buf := make([]byte, headerSize+bodyLen)

	buf[0] = magicByte1
	buf[1] = magicByte2
	buf[2] = wireVersion

	binary.BigEndian.PutUint64(buf[7:15], r.ID)
	binary.BigEndian.PutUint64(buf[15:23], uint64(r.Timestamp.UnixNano()))
	binary.BigEndian.PutUint16(buf[23:25], uint16(len(topicBytes)))
	binary.BigEndian.PutUint32(buf[25:29], uint32(len(r.Payload)))

	copy(buf[29:29+len(topicBytes)], topicBytes)
	copy(buf[29+len(topicBytes):], r.Payload)

	// Compute CRC32 over the payload and metadata (excluding magic and crc field itself).
	checksum := crc32.ChecksumIEEE(buf[7:])
	binary.BigEndian.PutUint32(buf[3:7], checksum)

	return buf, nil
}

// UnmarshalBinary decodes a record from binary bytes, verifying magic bytes and CRC32 checksum.
func (r *Record) UnmarshalBinary(data []byte) error {
	// 1. Valider que len(data) >= headerSize.
	if len(data) < headerSize {
		return ErrTruncatedData
	}

	// 2. Valider magic bytes et wire version.
	if data[0] != magicByte1 || data[1] != magicByte2 || data[2] != wireVersion {
		return fmt.Errorf("%w: invalid magic bytes or version", ErrCorruptRecord)
	}

	// 3. Extraire topicLen et payloadLen depuis le header.
	topicLen := int(binary.BigEndian.Uint16(data[23:25]))
	payloadLen := int(binary.BigEndian.Uint32(data[25:29]))

	// 4. Calculer totalLen := headerSize + topicLen + payloadLen.
	totalLen := headerSize + topicLen + payloadLen

	// 5. Vérifier if len(data) < totalLen { return ErrTruncatedData }.
	if len(data) < totalLen {
		return ErrTruncatedData
	}

	// 6. Calculer computedChecksum := crc32.ChecksumIEEE(data[7:totalLen]).
	expectedChecksum := binary.BigEndian.Uint32(data[3:7])
	computedChecksum := crc32.ChecksumIEEE(data[7:totalLen])

	// 7. Si expectedChecksum != computedChecksum { return fmt.Errorf("%w: expected 0x%x, calculated 0x%x", ErrCorruptRecord, expectedChecksum, computedChecksum) }.
	if expectedChecksum != computedChecksum {
		return fmt.Errorf("%w: expected 0x%x, calculated 0x%x", ErrCorruptRecord, expectedChecksum, computedChecksum)
	}

	// 8. Peupler r.ID, r.Timestamp, r.Topic, r.Checksum.
	r.ID = binary.BigEndian.Uint64(data[7:15])
	r.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[15:23])))
	r.Topic = string(data[headerSize : headerSize+topicLen])
	r.Checksum = expectedChecksum

	// 9. Effectuer une copie défensive pour r.Payload :
	payloadCopy := make([]byte, payloadLen)
	copy(payloadCopy, data[headerSize+topicLen:totalLen])
	r.Payload = payloadCopy

	return nil
}
