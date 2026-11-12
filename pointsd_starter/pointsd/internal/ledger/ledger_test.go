package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
)

const cap10k = 10_000

func award(t *testing.T, s ledger.Store, payment, member string, points int64, month string) int64 {
	t.Helper()
	got, err := s.Award(context.Background(), ledger.Entry{
		MemberID: member, PaymentID: payment, Kind: ledger.KindAward, Points: points, Month: month,
	}, cap10k)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAward_PaymentEarnsOnce(t *testing.T) {
	s := ledger.NewMemoryStore()

	if got := award(t, s, "pay_1", "mbr_1", 1800, "2026-10"); got != 1800 {
		t.Fatalf("first award = %d, want 1800", got)
	}
	if got := award(t, s, "pay_1", "mbr_1", 1800, "2026-10"); got != 0 {
		t.Fatalf("re-award = %d, want 0", got)
	}
	if b, _ := s.Balance(context.Background(), "mbr_1"); b != 1800 {
		t.Fatalf("balance = %d, want 1800", b)
	}
}

func TestAward_ClampsToMonthlyCap(t *testing.T) {
	s := ledger.NewMemoryStore()

	if got := award(t, s, "pay_1", "mbr_1", 9500, "2026-10"); got != 9500 {
		t.Fatalf("first award = %d, want 9500", got)
	}
	// Only 500 of the cap is left, so this payment earns 500.
	if got := award(t, s, "pay_2", "mbr_1", 1000, "2026-10"); got != 500 {
		t.Fatalf("capped award = %d, want 500", got)
	}
	// A different month has its own cap.
	if got := award(t, s, "pay_3", "mbr_1", 1000, "2026-11"); got != 1000 {
		t.Fatalf("next-month award = %d, want 1000", got)
	}
}

func TestAward_ConcurrentRedeliveryWritesOnce(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.Award(context.Background(), ledger.Entry{
				MemberID: "mbr_1", PaymentID: "pay_1", Kind: ledger.KindAward, Points: 2500, Month: "2026-10",
			}, cap10k)
		}()
	}
	wg.Wait()

	if b, _ := s.Balance(context.Background(), "mbr_1"); b != 2500 {
		t.Fatalf("balance = %d, want 2500 (awarded more than once)", b)
	}
}

func TestAward_ConcurrentPaymentsNeverExceedCap(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = s.Award(context.Background(), ledger.Entry{
				MemberID: "mbr_1", PaymentID: fmt.Sprintf("pay_%d", i), Kind: ledger.KindAward, Points: 1000, Month: "2026-10",
			}, cap10k)
		}(i)
	}
	wg.Wait()

	if b, _ := s.Balance(context.Background(), "mbr_1"); b != cap10k {
		t.Fatalf("balance = %d, want %d (cap must hold under concurrency)", b, cap10k)
	}
}

func TestReverse_ClawsBackOnlyAwardedPoints(t *testing.T) {
	s := ledger.NewMemoryStore()
	ctx := context.Background()

	award(t, s, "pay_1", "mbr_1", 9500, "2026-10")
	award(t, s, "pay_2", "mbr_1", 1000, "2026-10") // capped to 500

	if err := s.Reverse(ctx, ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_2", Kind: ledger.KindReversal, Month: "2026-10"}); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.Balance(ctx, "mbr_1"); b != 9500 {
		t.Fatalf("balance = %d, want 9500 (reversal must remove only the 500 awarded)", b)
	}
}

func TestReverse_IsIdempotent(t *testing.T) {
	s := ledger.NewMemoryStore()
	ctx := context.Background()

	award(t, s, "pay_1", "mbr_1", 2000, "2026-10")
	e := ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_1", Kind: ledger.KindReversal, Month: "2026-10"}
	if err := s.Reverse(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := s.Reverse(ctx, e); err != nil {
		t.Fatal(err)
	}
	if b, _ := s.Balance(ctx, "mbr_1"); b != 0 {
		t.Fatalf("balance = %d, want 0 after a single reversal", b)
	}
}

func TestReverse_UnknownPayment(t *testing.T) {
	s := ledger.NewMemoryStore()
	ctx := context.Background()

	err := s.Reverse(ctx, ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_never", Kind: ledger.KindReversal, Month: "2026-10"})
	if !errors.Is(err, ledger.ErrUnknownPayment) {
		t.Fatalf("err = %v, want ErrUnknownPayment", err)
	}
	if b, _ := s.Balance(ctx, "mbr_1"); b != 0 {
		t.Fatalf("balance = %d, want 0", b)
	}
}
