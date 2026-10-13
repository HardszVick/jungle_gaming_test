package platform

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestUUIDv7GeneratorProducesOrderedRFCUUIDs(t *testing.T) {
	fixed := time.Date(2026, time.October, 10, 20, 30, 0, 123_000_000, time.FixedZone("test", 2*60*60))
	generator := newDeterministicUUIDGenerator(ClockFunc(func() time.Time { return fixed }))

	var previous uuid.UUID
	for index := 0; index < 100; index++ {
		current, err := generator.New()
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if current.Version() != 7 {
			t.Fatalf("UUID version = %d, want 7", current.Version())
		}
		if current.Variant() != uuid.RFC4122 {
			t.Fatalf("UUID variant = %v, want RFC 4122", current.Variant())
		}
		if index > 0 && bytes.Compare(previous[:], current[:]) >= 0 {
			t.Fatalf("UUID %s is not after %s", current, previous)
		}
		previous = current
	}
}

func TestUUIDv7GeneratorRemainsOrderedWhenClockMovesBackward(t *testing.T) {
	times := []time.Time{
		time.UnixMilli(2_000),
		time.UnixMilli(1_000),
	}
	index := 0
	generator := newDeterministicUUIDGenerator(ClockFunc(func() time.Time {
		value := times[index]
		index++
		return value
	}))

	first, err := generator.New()
	if err != nil {
		t.Fatalf("first New() error = %v", err)
	}
	second, err := generator.New()
	if err != nil {
		t.Fatalf("second New() error = %v", err)
	}
	if bytes.Compare(first[:], second[:]) >= 0 {
		t.Fatalf("second UUID %s is not after first UUID %s", second, first)
	}
}

func TestUUIDv7GeneratorIsSafeAndUniqueConcurrently(t *testing.T) {
	const count = 256
	generator := newDeterministicUUIDGenerator(ClockFunc(func() time.Time { return time.UnixMilli(1_000) }))
	ids := make(chan uuid.UUID, count)
	errorsChannel := make(chan error, count)

	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			id, err := generator.New()
			if err != nil {
				errorsChannel <- err
				return
			}
			ids <- id
		}()
	}
	wait.Wait()
	close(ids)
	close(errorsChannel)

	for err := range errorsChannel {
		t.Errorf("New() error = %v", err)
	}
	seen := make(map[uuid.UUID]struct{}, count)
	for id := range ids {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate UUID generated: %s", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("generated %d UUIDs, want %d", len(seen), count)
	}
}

func TestUUIDv7GeneratorReportsEntropyFailure(t *testing.T) {
	generator := NewUUIDv7Generator(ClockFunc(func() time.Time { return time.UnixMilli(1_000) }))
	generator.random = errorReader{}

	if id, err := generator.New(); err == nil || id != uuid.Nil {
		t.Fatalf("New() = (%s, %v), want nil UUID and error", id, err)
	}
}

func TestUUIDv7GeneratorRejectsTimestampBeforeEpoch(t *testing.T) {
	generator := newDeterministicUUIDGenerator(ClockFunc(func() time.Time { return time.UnixMilli(-1) }))
	if id, err := generator.New(); err == nil || id != uuid.Nil {
		t.Fatalf("New() = (%s, %v), want nil UUID and error", id, err)
	}
}

func newDeterministicUUIDGenerator(clock Clock) *UUIDv7Generator {
	generator := NewUUIDv7Generator(clock)
	generator.random = bytes.NewReader(make([]byte, 10*1024))
	return generator
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy unavailable")
}
