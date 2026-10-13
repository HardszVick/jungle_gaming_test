package platform

import (
	"crypto/rand"
	"errors"
	"io"
	"sync"

	"github.com/google/uuid"
)

const maxUUIDv7Milliseconds = 1<<48 - 1

type UUIDGenerator interface {
	New() (uuid.UUID, error)
}

// UUIDv7Generator emits RFC 9562 UUID v7 values. A per-generator logical
// timestamp and sequence preserve byte ordering if the clock stalls or moves back.
type UUIDv7Generator struct {
	clock  Clock
	random io.Reader

	mu          sync.Mutex
	initialized bool
	lastMillis  int64
	sequence    uint16
}

func NewUUIDv7Generator(clock Clock) *UUIDv7Generator {
	if clock == nil {
		clock = NewClock()
	}
	return &UUIDv7Generator{clock: clock, random: rand.Reader}
}

func (generator *UUIDv7Generator) New() (uuid.UUID, error) {
	if generator == nil || generator.clock == nil || generator.random == nil {
		return uuid.Nil, errors.New("platform: UUID v7 generator is not initialized")
	}

	generator.mu.Lock()
	defer generator.mu.Unlock()

	milliseconds := generator.clock.Now().UTC().UnixMilli()
	if milliseconds < 0 || milliseconds > maxUUIDv7Milliseconds {
		return uuid.Nil, errors.New("platform: UUID v7 timestamp is outside its 48-bit range")
	}

	var entropy [10]byte
	if _, err := io.ReadFull(generator.random, entropy[:]); err != nil {
		return uuid.Nil, errors.New("platform: generate UUID v7 entropy: " + err.Error())
	}

	if !generator.initialized || milliseconds > generator.lastMillis {
		generator.lastMillis = milliseconds
		generator.sequence = uint16(entropy[0]&0x0f)<<8 | uint16(entropy[1])
		generator.initialized = true
	} else if generator.sequence < 0x0fff {
		generator.sequence++
	} else {
		if generator.lastMillis == maxUUIDv7Milliseconds {
			return uuid.Nil, errors.New("platform: UUID v7 timestamp sequence exhausted")
		}
		generator.lastMillis++
		generator.sequence = 0
	}

	milliseconds = generator.lastMillis
	id := uuid.UUID{
		byte(milliseconds >> 40),
		byte(milliseconds >> 32),
		byte(milliseconds >> 24),
		byte(milliseconds >> 16),
		byte(milliseconds >> 8),
		byte(milliseconds),
		0x70 | byte(generator.sequence>>8),
		byte(generator.sequence),
		0x80 | (entropy[2] & 0x3f),
		entropy[3], entropy[4], entropy[5], entropy[6], entropy[7], entropy[8], entropy[9],
	}
	return id, nil
}
