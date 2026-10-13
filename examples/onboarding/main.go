// Command onboarding registers create-account on the accounts worker.
// Start a server, then run with RUNCOIL_ENDPOINT and RUNCOIL_TOKEN (or defaults).
// EMAIL_FAIL_ONCE=true makes the first delivery for each account fail for retry.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)

type onboardingInput struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
}

type onboardingOutput struct {
	AccountID string `json:"account_id"`
	MessageID string `json:"message_id"`
}

type onboarding struct {
	accounts *accountService
	emails   *emailService
	logger   *slog.Logger
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failOnce, err := strconv.ParseBool(envOr("EMAIL_FAIL_ONCE", "false"))
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	accounts := &accountService{accounts: make(map[string]account), logger: logger}
	emails := &emailService{
		messages:  make(map[string]string),
		attempted: make(map[string]bool),
		failOnce:  failOnce,
		logger:    logger,
	}
	handler := &onboarding{accounts: accounts, emails: emails, logger: logger}

	client, err := runcoil.NewClient(
		runcoil.Endpoint(envOr("RUNCOIL_ENDPOINT", "http://localhost:7070")),
		runcoil.Token(envOr("RUNCOIL_TOKEN", "dev-token")),
		runcoil.Logger(logger),
	)
	if err != nil {
		return err
	}

	worker, err := runcoil.NewWorker(client, runcoil.WorkerID("accounts"))
	if err != nil {
		return err
	}

	if err := worker.NewTask("create-account", runcoil.HandlerFunc(handler.createAccount),
		runcoil.Retry(runcoil.RetryPolicy{MaxAttempts: 3, Delays: []time.Duration{time.Second}}),
	); err != nil {
		return err
	}

	if err := worker.Start(ctx); err != nil {
		return err
	}

	logger.InfoContext(ctx, "worker started", slog.String("worker_id", "accounts"))
	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := worker.Shutdown(drainCtx); err != nil {
		return err
	}

	logger.InfoContext(drainCtx, "worker stopped")
	return nil
}

func (o *onboarding) createAccount(ctx context.Context, in onboardingInput) (onboardingOutput, error) {
	if strings.TrimSpace(in.AccountID) == "" || strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.Email) == "" {
		return onboardingOutput{}, runcoil.Permanent(errors.New("account_id, name, and email are required"))
	}

	info := runcoil.Info(ctx)
	o.logger.InfoContext(ctx, "onboarding started",
		slog.String("run_id", info.RunID),
		slog.Int("attempt", info.Attempt),
		slog.String("account_id", in.AccountID),
	)

	created, err := step.Do(ctx, "create-account", func(ctx context.Context) (account, error) {
		return o.accounts.Create(ctx, in.AccountID, in.Name, in.Email)
	})
	if err != nil {
		return onboardingOutput{}, err
	}

	messageID, err := step.Do(ctx, "send-welcome-email", func(ctx context.Context) (string, error) {
		return o.emails.SendWelcome(ctx, created)
	})
	if err != nil {
		// Keep the account when delivery fails finally; fail the Run.
		return onboardingOutput{}, err
	}

	return onboardingOutput{AccountID: created.ID, MessageID: messageID}, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
