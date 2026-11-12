// Package rewards turns payment events into points ledger entries.
package rewards

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
)

// ErrUnknownPayment is returned for a reversal of a payment that was never awarded.
var ErrUnknownPayment = ledger.ErrUnknownPayment

type Handler struct {
	store      ledger.Store
	policy     Policy
	deadLetter pubsub.Publisher
	log        *slog.Logger
}

func NewHandler(store ledger.Store, policy Policy, deadLetter pubsub.Publisher, log *slog.Logger) *Handler {
	return &Handler{store: store, policy: policy, deadLetter: deadLetter, log: log}
}

// Handle processes one delivery. The subscriber calls it concurrently
// (MaxOutstandingMessages per replica), and pointsd runs several replicas.
//
// The message is settled exactly once. A transient ledger failure Nacks so the
// broker redelivers it; a reversal of a payment that never earned points is
// Ack'd and logged, because retrying it cannot change the outcome. Both
// deliveries of a redelivered event are safe: the dedup lives in the ledger.
func (h *Handler) Handle(ctx context.Context, msg pubsub.Message) {
	ev, err := events.Decode(msg.Data())
	if err != nil {
		h.log.Warn("dead-lettering invalid event", "msg_id", msg.ID(), "err", err)
		if err := h.deadLetter.Publish(ctx, msg.Data(), map[string]string{"reason": err.Error()}); err != nil {
			h.log.Error("dead-letter publish failed", "msg_id", msg.ID(), "err", err)
			msg.Nack()
			return
		}
		msg.Ack()
		return
	}

	if err := h.process(ctx, ev); err != nil {
		if errors.Is(err, ErrUnknownPayment) {
			// Nothing to claw back. Ack it; finance reconciles from the logs.
			h.log.Warn("reversal for payment with no award", "payment_id", ev.PaymentID, "msg_id", msg.ID())
			msg.Ack()
			return
		}
		h.log.Error("processing failed, asking the broker to redeliver", "payment_id", ev.PaymentID, "type", ev.Type, "err", err)
		msg.Nack()
		return
	}
	msg.Ack()
}

func (h *Handler) process(ctx context.Context, ev events.Payment) error {
	switch ev.Type {
	case events.TypeSettled:
		return h.award(ctx, ev)
	case events.TypeReversed:
		return h.reverse(ctx, ev)
	}
	return nil
}

func (h *Handler) award(ctx context.Context, ev events.Payment) error {
	// The ledger caps the award and dedups the payment in one transaction; the
	// handler only says what the payment would be worth uncapped.
	_, err := h.store.Award(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindAward,
		Points:    h.policy.Points(ev.AmountCents),
		Month:     Month(ev.OccurredAt),
	}, h.policy.MonthlyCap)
	return err
}

func (h *Handler) reverse(ctx context.Context, ev events.Payment) error {
	// The ledger reads the award it is undoing, so a capped award is only
	// clawed back for the points it was actually credited.
	return h.store.Reverse(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindReversal,
		Month:     Month(ev.OccurredAt),
	})
}
