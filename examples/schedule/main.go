// Command schedule emails yesterday's sales report at 09:00 UTC, skipping overlap.
// REPORT_INTERVAL=3s replaces the daily schedule for development. Repeated ticks
// for the same reporting day reuse the same simulated email delivery.
// Start a server, then run with RUNCOIL_ENDPOINT and RUNCOIL_TOKEN (or defaults).
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
	Report    dailyReport `json:"report"`
	MessageID string      `json:"message_id"`
}

type reporting struct {
	reports *reportService
	emails  *emailService
	logger  *slog.Logger
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sched := schedule.Cron("0 9 * * *", schedule.SkipOverlap())
	if interval := os.Getenv("REPORT_INTERVAL"); interval != "" {
		duration, err := time.ParseDuration(interval)
		if err != nil {
			return err
		}

		sched = schedule.Every(duration, schedule.SkipOverlap())
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// Two sample sales from yesterday, in UTC; other reporting days may be empty.
	yesterday := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	reports := &reportService{
		sales: []sale{
			{OccurredAt: yesterday.Add(10 * time.Hour), TotalCents: 2500},
			{OccurredAt: yesterday.Add(14 * time.Hour), TotalCents: 3900},
		},
		logger: logger,
	}
	emails := &emailService{messages: make(map[string]string), logger: logger}
	handler := &reporting{reports: reports, emails: emails, logger: logger}

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

	if err := worker.NewTask("daily-report", runcoil.HandlerFunc(handler.dailyReport),
		runcoil.Schedule(sched),
		runcoil.Retry(runcoil.RetryPolicy{MaxAttempts: 3}),
	); err != nil {
		return err
	}

	if err := worker.Start(ctx); err != nil {
		return err
	}

	logger.InfoContext(ctx, "worker started", slog.String("worker_id", "reports"))
	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := worker.Shutdown(drainCtx); err != nil {
		return err
	}

	logger.InfoContext(drainCtx, "worker stopped")
	return nil
}

// The scheduled Run has no input. Its original tick identifies the report day,
// so a retry after midnight still builds the same report.
func (r *reporting) dailyReport(ctx context.Context, _ struct{}) (reportResult, error) {
	info := runcoil.Info(ctx)
	day := info.ScheduledAt.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)
	r.logger.InfoContext(ctx, "daily report started",
		slog.String("run_id", info.RunID),
		slog.Int("attempt", info.Attempt),
		slog.Time("scheduled_at", info.ScheduledAt),
		slog.String("report_day", day.Format(time.DateOnly)),
	)

	report, err := step.Do(ctx, "build-daily-report", func(ctx context.Context) (dailyReport, error) {
		return r.reports.BuildDaily(ctx, day)
	})
	if err != nil {
		return reportResult{}, err
	}

	messageID, err := step.Do(ctx, "send-report-email", func(ctx context.Context) (string, error) {
		return r.emails.SendReport(ctx, report)
	})
	if err != nil {
		return reportResult{}, err
	}

	return reportResult{Report: report, MessageID: messageID}, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
