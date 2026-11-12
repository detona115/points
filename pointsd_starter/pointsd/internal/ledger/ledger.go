// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrUnknownPayment is returned by Reverse for a payment that has no award to
// claw back. Retrying cannot change the outcome.
var ErrUnknownPayment = errors.New("ledger: reversal for payment with no award")

// Store is the points ledger.
//
// Award and Reverse are the only writers the rewards handler needs, and each is
// a single transaction that carries its own guarantee: a payment is awarded at
// most once, the monthly cap cannot be exceeded, and a reversal removes exactly
// what its award wrote. Keeping those guarantees inside the store is what makes
// them hold across replicas; in production they live in Postgres, not in one
// process's memory. Different payments for different members do not block each
// other.
type Store interface {
	// Award records e as the award for e.PaymentID at most once, enforcing the
	// monthly cap over the member's awards in e.Month. Callers racing on the
	// same payment, or on the same member-month, are resolved by the store. It
	// returns the points actually written: 0 when the payment already has an
	// award, or when the month is already at the cap.
	Award(ctx context.Context, e Entry, monthlyCap int64) (int64, error)

	// Reverse records the reversal for e.PaymentID, removing exactly the points
	// its award wrote. It is idempotent: a second reversal is a no-op. It
	// returns ErrUnknownPayment when the payment has no award.
	Reverse(ctx context.Context, e Entry) error

	// Append writes e without any dedup or cap checks.
	Append(ctx context.Context, e Entry) error

	// Balance returns the sum of the points on the member's entries.
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

// Award mirrors the Postgres award transaction: it takes the member-month lock,
// re-reads what the month already awarded, and only then writes the entry. The
// critical section is in memory, so it is fast; the simulated round trip sits
// outside the lock and never serializes different members.
func (s *MemoryStore) Award(ctx context.Context, e Entry, monthlyCap int64) (int64, error) {
	if err := s.roundTrip(ctx, "award"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, x := range s.entries {
		if x.PaymentID == e.PaymentID && x.Kind == KindAward {
			return 0, nil // this payment already earned
		}
	}

	var used int64
	for _, x := range s.entries {
		if x.MemberID == e.MemberID && x.Month == e.Month && x.Kind == KindAward {
			used += x.Points
		}
	}
	if room := monthlyCap - used; e.Points > room {
		e.Points = room
	}
	if e.Points < 0 {
		e.Points = 0
	}

	s.entries = append(s.entries, e)
	return e.Points, nil
}

// Reverse mirrors the Postgres reversal transaction: it reads the award row
// under the same lock that Award takes and negates exactly what that row holds,
// so a capped award is only ever clawed back for the points it was credited.
func (s *MemoryStore) Reverse(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	awarded, found := int64(0), false
	for _, x := range s.entries {
		if x.PaymentID == e.PaymentID && x.Kind == KindAward {
			awarded, found = x.Points, true
			break
		}
	}
	if !found {
		return ErrUnknownPayment
	}
	for _, x := range s.entries {
		if x.PaymentID == e.PaymentID && x.Kind == KindReversal {
			return nil // already reversed
		}
	}

	e.Kind = KindReversal
	e.Points = -awarded
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
