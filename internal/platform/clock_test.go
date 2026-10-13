package platform

import (
	"testing"
	"time"
)

func TestUTCClockReturnsUTC(t *testing.T) {
	now := NewClock().Now()
	_, offset := now.Zone()
	if now.Location() != time.UTC || offset != 0 {
		t.Fatalf("NewClock().Now() = %v in %v, want UTC", now, now.Location())
	}
}

func TestClockFuncNormalizesToUTC(t *testing.T) {
	location := time.FixedZone("test", -3*60*60)
	want := time.Date(2026, time.October, 10, 20, 30, 0, 0, time.UTC)
	clock := ClockFunc(func() time.Time { return want.In(location) })

	got := clock.Now()
	if !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("ClockFunc.Now() = %v in %v, want %v in UTC", got, got.Location(), want)
	}
}
