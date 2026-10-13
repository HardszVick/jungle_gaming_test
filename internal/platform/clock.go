package platform

import "time"

// Clock makes time an explicit dependency of domain and application services.
type Clock interface {
	Now() time.Time
}

type UTCClock struct{}

func NewClock() Clock {
	return UTCClock{}
}

func (UTCClock) Now() time.Time {
	return time.Now().UTC()
}

// ClockFunc is a compact clock implementation for tests and adapters.
type ClockFunc func() time.Time

func (clock ClockFunc) Now() time.Time {
	return clock().UTC()
}
