package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type account struct {
	ID    string `json:"account_id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// These simulated services keep data only in this process. Real account storage
// and email delivery must enforce these keys durably, including across workers.
type accountService struct {
	mu       sync.Mutex
	accounts map[string]account
	logger   *slog.Logger
}

func (s *accountService) Create(ctx context.Context, id, name, email string) (account, error) {
	if err := simulatedIO(ctx); err != nil {
		return account{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return account{}, err
	}

	created := account{ID: id, Name: name, Email: email}
	if existing, ok := s.accounts[id]; ok {
		if existing != created {
			return account{}, errors.New("account_id already belongs to different account details")
		}

		s.logger.InfoContext(ctx, "existing account reused", slog.String("account_id", id))
		return existing, nil
	}

	s.accounts[id] = created
	s.logger.InfoContext(ctx, "simulated account created", slog.String("account_id", id))
	return created, nil
}

type emailService struct {
	mu        sync.Mutex
	messages  map[string]string
	attempted map[string]bool
	failOnce  bool
	logger    *slog.Logger
}

func (s *emailService) SendWelcome(ctx context.Context, recipient account) (string, error) {
	if err := simulatedIO(ctx); err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	key := "welcome/" + recipient.ID
	if messageID, ok := s.messages[key]; ok {
		s.logger.InfoContext(ctx, "welcome delivery reused", slog.String("delivery_key", key))
		return messageID, nil
	}

	if s.failOnce && !s.attempted[key] {
		s.attempted[key] = true
		s.logger.WarnContext(ctx, "simulated email service unavailable", slog.String("delivery_key", key))
		return "", errors.New("simulated email service unavailable")
	}

	messageID := "welcome_" + recipient.ID
	s.messages[key] = messageID
	s.logger.InfoContext(ctx, "simulated welcome email sent",
		slog.String("delivery_key", key),
		slog.String("message_id", messageID),
		slog.String("recipient_email", recipient.Email),
	)
	return messageID, nil
}

// simulatedIO stands in for a cancellable storage or provider request.
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
