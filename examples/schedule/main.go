// Command schedule runs a worker with worker ID "reports" and one scheduled Task,
// "report". The server starts a Run every 3 seconds. A Run carries no input; the
// region is a field of the handler's struct. The Run does one Step that sleeps
// for REPORT_SLEEP (default 1s), so the overlap rule can be seen by changing it:
// with REPORT_SLEEP above 3s, a tick that comes while the previous Run is still
// running creates no Run, because the schedule uses SkipOverlap.
//
// Start a server, then run this program with RUNCOIL_ENDPOINT and RUNCOIL_TOKEN
// set (or the defaults). Stop it with Ctrl-C; it drains running handlers for 30 seconds.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/schedule"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)

type reportResult struct {
	Region string `json:"region"`
	Rows   int    `json:"rows"`
}

type reporter struct {
	logger *slog.Logger
	region string
	sleep  time.Duration
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sleep, err := time.ParseDuration(envOr("REPORT_SLEEP", "1s"))
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

	worker, err := runcoil.NewWorker(client, runcoil.WorkerID("reports"))
	if err != nil {
		return err
	}

	r := &reporter{logger: logger, region: "eu", sleep: sleep}
	if err := worker.NewTask("report", runcoil.HandlerFunc(r.report),
		runcoil.Schedule(schedule.Every(3*time.Second, schedule.SkipOverlap())),
	); err != nil {
		return err
	}

	if err := worker.Start(ctx); err != nil {
		return err
	}

	logger.Info("worker started",
		slog.String("worker_id", "reports"),
		slog.Duration("report_sleep", sleep),
	)

	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := worker.Shutdown(drainCtx); err != nil {
		return err
	}

	logger.Info("worker stopped")
	return nil
}

// report is the handler of a scheduled Run. Its input is the zero value of struct{}.
func (r *reporter) report(ctx context.Context, _ struct{}) (reportResult, error) {
	info := runcoil.Info(ctx)
	logger := r.logger.With(slog.String("run_id", info.RunID))
	logger.InfoContext(ctx, "report started",
		slog.Int("attempt", info.Attempt),
		slog.Time("scheduled_at", info.ScheduledAt),
		slog.String("region", r.region),
	)

	rows, err := step.Do(ctx, "build", func(ctx context.Context) (int, error) {
		select {
		case <-time.After(r.sleep):
		case <-ctx.Done():
			return 0, ctx.Err()
		}

		return 42, nil
	})
	if err != nil {
		return reportResult{}, err
	}

	logger.InfoContext(ctx, "report built", slog.Int("rows", rows))

	return reportResult{Region: r.region, Rows: rows}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
