package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type sale struct {
	OccurredAt time.Time
	TotalCents int64
}

type dailyReport struct {
	Day        string `json:"day"`
	Orders     int    `json:"orders"`
	TotalCents int64  `json:"total_cents"`
}

// The sample sales are immutable after startup; this service reads them as a
// stand-in for a cancellable query against application storage.
type reportService struct {
	sales  []sale
	logger *slog.Logger
}

func (s *reportService) BuildDaily(ctx context.Context, day time.Time) (dailyReport, error) {
	if err := simulatedIO(ctx); err != nil {
		return dailyReport{}, err
	}

	report := dailyReport{Day: day.Format(time.DateOnly)}
	end := day.AddDate(0, 0, 1)
	for _, sale := range s.sales {
		if !sale.OccurredAt.Before(day) && sale.OccurredAt.Before(end) {
			report.Orders++
			report.TotalCents += sale.TotalCents
		}
	}

	s.logger.InfoContext(ctx, "simulated sales report built",
		slog.String("report_day", report.Day),
		slog.Int("orders", report.Orders),
		slog.Int64("total_cents", report.TotalCents),
	)
	return report, nil
}

// Delivery deduplication lasts only within this process. A real email provider
// must enforce the reporting-day key durably, including across workers.
type emailService struct {
	mu       sync.Mutex
	messages map[string]string
	logger   *slog.Logger
}

func (s *emailService) SendReport(ctx context.Context, report dailyReport) (string, error) {
	if err := simulatedIO(ctx); err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	key := "daily-report/" + report.Day
	if messageID, ok := s.messages[key]; ok {
		s.logger.InfoContext(ctx, "report delivery reused", slog.String("delivery_key", key))
		return messageID, nil
	}

	messageID := "report_" + report.Day
	s.messages[key] = messageID
	s.logger.InfoContext(ctx, "simulated report email sent",
		slog.String("delivery_key", key),
		slog.String("message_id", messageID),
		slog.String("report_day", report.Day),
		slog.Int("orders", report.Orders),
		slog.Int64("total_cents", report.TotalCents),
	)
	return messageID, nil
}

func simulatedIO(ctx context.Context) error {
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
