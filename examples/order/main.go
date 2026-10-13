// Command order runs a worker with worker ID "orders" and one Task, "process-order".
// Each Run reserves stock, charges the card, and sends a receipt. When the
// charge fails for good, the Run releases the stock as a compensation.
//
// The input's fail_charge field makes the charge fail in a chosen way:
//
//	never     the charge succeeds
//	always    every try fails with a plain error; the Step fails for good after 3 tries
//	once      the first try fails, the next succeeds (a counter in process memory)
//	declined  the charge fails with a permanent error; no retry
//
// Every Step body logs when it really runs. A retried Run replays the Steps
// that succeeded, so their bodies do not log again.
//
// CHARGE_SLEEP (default 0) makes the charge Step wait before it charges, so a
// worker can be killed in the middle of a Step by hand. The wait ends early when
// the attempt is cancelled.
//
// Start a server, then run this program with RUNCOIL_ENDPOINT and RUNCOIL_TOKEN
// set (or the defaults). Stop it with Ctrl-C; it drains running handlers for 30 seconds.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)

type orderInput struct {
	OrderID    string `json:"order_id"`
	FailCharge string `json:"fail_charge"`
}

type orderOutput struct {
	ReservationID string `json:"reservation_id"`
	ChargeID      string `json:"charge_id"`
	ReceiptID     string `json:"receipt_id"`
}

type orders struct {
	logger      *slog.Logger
	chargeSleep time.Duration

	mu    sync.Mutex
	tries map[string]int // charge tries per order, for the "once" mode
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	chargeSleep, err := time.ParseDuration(envOr("CHARGE_SLEEP", "0"))
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	client, err := runcoil.NewClient(
		runcoil.Endpoint(envOr("RUNCOIL_ENDPOINT", "http://localhost:7070")),
		runcoil.Token(envOr("RUNCOIL_TOKEN", "dev-token")),
		runcoil.Logger(logger),
	)
	if err != nil {
		return err
	}

	worker, err := runcoil.NewWorker(client, runcoil.WorkerID("orders"))
	if err != nil {
		return err
	}

	o := &orders{logger: logger, chargeSleep: chargeSleep, tries: map[string]int{}}
	if err := worker.NewTask("process-order", runcoil.HandlerFunc(o.processOrder),
		runcoil.Retry(runcoil.RetryPolicy{MaxAttempts: 3}),
	); err != nil {
		return err
	}

	if err := worker.Start(ctx); err != nil {
		return err
	}

	logger.Info("worker started", slog.String("worker_id", "orders"))

	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := worker.Shutdown(drainCtx); err != nil {
		return err
	}

	logger.Info("worker stopped")
	return nil
}

func (o *orders) processOrder(ctx context.Context, in orderInput) (orderOutput, error) {
	info := runcoil.Info(ctx)
	o.logger.InfoContext(ctx, "handler running",
		slog.String("run_id", info.RunID),
		slog.Int("attempt", info.Attempt),
		slog.String("order_id", in.OrderID),
	)

	reservation, err := step.Do(ctx, "reserve-stock", func(ctx context.Context) (string, error) {
		o.logger.InfoContext(ctx, "step body executed", slog.String("step_id", "reserve-stock"))
		return "res_" + in.OrderID, nil
	})
	if err != nil {
		return orderOutput{}, err
	}

	charge, err := step.Do(ctx, "charge", func(ctx context.Context) (string, error) {
		o.logger.InfoContext(ctx, "step body executed",
			slog.String("step_id", "charge"),
			slog.Int("attempt", info.Attempt),
		)
		if o.chargeSleep > 0 {
			select {
			case <-time.After(o.chargeSleep):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		return o.charge(in)
	})
	if err != nil {
		// Compensation: the charge failed for good, so give the stock back.
		var se *step.Error
		if !errors.As(err, &se) {
			return orderOutput{}, err
		}

		if _, rerr := step.Do(ctx, "release-stock", func(ctx context.Context) (struct{}, error) {
			o.logger.InfoContext(ctx, "step body executed", slog.String("step_id", "release-stock"))
			return struct{}{}, nil
		}); rerr != nil {
			return orderOutput{}, rerr
		}

		return orderOutput{ReservationID: reservation}, err
	}

	receipt, err := step.Do(ctx, "send-receipt", func(ctx context.Context) (string, error) {
		o.logger.InfoContext(ctx, "step body executed", slog.String("step_id", "send-receipt"))
		return "rc_" + in.OrderID, nil
	})
	if err != nil {
		return orderOutput{}, err
	}

	return orderOutput{ReservationID: reservation, ChargeID: charge, ReceiptID: receipt}, nil
}

// charge is the payment call. Its failure modes come from the input.
func (o *orders) charge(in orderInput) (string, error) {
	switch in.FailCharge {
	case "", "never":
		return "ch_" + in.OrderID, nil
	case "always":
		return "", errors.New("payment gateway timed out")
	case "once":
		o.mu.Lock()
		o.tries[in.OrderID]++
		n := o.tries[in.OrderID]
		o.mu.Unlock()
		if n == 1 {
			return "", errors.New("payment gateway timed out")
		}

		return "ch_" + in.OrderID, nil
	case "declined":
		return "", runcoil.Permanent(errors.New("card declined"))
	default:
		return "", runcoil.Permanent(errors.New("unknown fail_charge mode: " + in.FailCharge))
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
