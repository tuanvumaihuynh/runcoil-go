package main

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentDeliveryFailsOnceAndDeduplicates(t *testing.T) {
	service := &emailService{
		messages:  make(map[string]string),
		attempted: make(map[string]bool),
		failOnce:  true,
		logger:    slog.New(slog.DiscardHandler),
	}
	recipient := account{ID: "acct-ada", Name: "Ada", Email: "ada@example.com"}
	type result struct {
		messageID string
		err       error
	}

	const calls = 12
	results := make(chan result, calls)
	var group sync.WaitGroup
	for range calls {
		group.Go(func() {
			messageID, err := service.SendWelcome(context.Background(), recipient)
			results <- result{messageID: messageID, err: err}
		})
	}

	group.Wait()
	close(results)

	failures := 0
	for result := range results {
		if result.err != nil {
			require.EqualError(t, result.err, "simulated email service unavailable")
			failures++
			continue
		}

		require.Equal(t, "welcome_acct-ada", result.messageID)
	}

	require.Equal(t, 1, failures)
	require.Len(t, service.messages, 1)

	_, err := service.SendWelcome(context.Background(), account{ID: "acct-grace"})
	require.EqualError(t, err, "simulated email service unavailable")
}

func TestConcurrentAccountCreationKeepsOriginalDetails(t *testing.T) {
	service := &accountService{
		accounts: make(map[string]account),
		logger:   slog.New(slog.DiscardHandler),
	}
	errors := make(chan error, 2)
	var group sync.WaitGroup
	for _, name := range []string{"Ada", "Grace"} {
		group.Go(func() {
			_, err := service.Create(context.Background(), "shared-id", name, "same@example.com")
			errors <- err
		})
	}

	group.Wait()
	close(errors)

	failures := 0
	for err := range errors {
		if err != nil {
			require.EqualError(t, err, "account_id already belongs to different account details")
			failures++
		}
	}

	require.Equal(t, 1, failures)
	require.Len(t, service.accounts, 1)
}

func TestCancelledDeliveryDoesNotConsumeFailure(t *testing.T) {
	service := &emailService{
		messages:  make(map[string]string),
		attempted: make(map[string]bool),
		failOnce:  true,
		logger:    slog.New(slog.DiscardHandler),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.SendWelcome(ctx, account{ID: "acct-ada"})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, service.attempted)
	require.Empty(t, service.messages)
}
