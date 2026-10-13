# runcoil-go

The Go SDK for [Runcoil](https://github.com/tuanvumaihuynh/runcoil).
Run background work in your own application process and record progress between
Steps. For example, create an account through your account service, then send a
welcome email through your email service. If delivery fails, a retry can reuse
the recorded account result.

**Status: pre-release.** The API may change before version 1.0. This SDK runs
workers; callers currently invoke Tasks through the server API.

## Install

Use Go 1.26 or newer. Until a version is tagged:

```sh
go get github.com/tuanvumaihuynh/runcoil-go@main
```

Go records the resolved revision in your module. The SDK pins its schema
dependency; no separate schema checkout or `go.work` is needed.

## Create an account and send a welcome email

A Task is a typed handler, `func(context.Context, In) (Out, error)`. Its input
and output are JSON. A worker registers it with the server and runs its code.

The [onboarding example](examples/onboarding/main.go) wires two application
services into a handler. Each side effect has its own `step.Do`: the account
returned by the first Step becomes the input to the email call. The services
themselves have no dependency on Runcoil.

This is the complete runnable program. Its account storage and email delivery
are simulated in memory, with cancellable latency in place of external I/O.
Replace those services with your application's implementations.

`main.go`:

```go
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
```

<details>
<summary>services.go — simulated account storage and email delivery</summary>

```go
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
```

</details>

## Run it

Start a [Runcoil server](https://github.com/tuanvumaihuynh/runcoil#quick-start),
then run the worker from a checkout of this repository:

```sh
go run ./examples/onboarding
```

Examples read `RUNCOIL_ENDPOINT` (default `http://localhost:7070`) and
`RUNCOIL_TOKEN` (default `dev-token`). Stop with Ctrl-C to drain running
handlers for up to 30 seconds.

In another terminal, invoke worker `accounts`, Task `create-account`:

```sh
curl --fail-with-body -sS http://localhost:7070/runcoil.v1.RunService/Invoke \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{"workerId":"accounts","taskId":"create-account","input":"eyJhY2NvdW50X2lkIjoiYWNjdC1hZGEiLCJuYW1lIjoiQWRhIiwiZW1haWwiOiJhZGFAZXhhbXBsZS5jb20ifQ==","idempotencyKey":"onboarding-acct-ada-1"}'
# {"runId":"..."}
```

The base64 `input` encodes:

```json
{"account_id":"acct-ada","name":"Ada","email":"ada@example.com"}
```

Use the returned Run ID to wait for the result and read its Steps:

```sh
curl --fail-with-body -sS http://localhost:7070/runcoil.v1.RunService/WaitRun \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{"runId":"<run id>","includeSteps":true}'
```

On success, the response's `run.output` is base64-encoded JSON:

```json
{"account_id":"acct-ada","message_id":"welcome_acct-ada"}
```

If `timedOut` is true, repeat `WaitRun`. Repeating an invoke with the same
idempotency key and input returns the original Run; use a new key for a new Run.

## Steps and retries

When a Step succeeds and its result is recorded, later attempts return that
result without calling the Step body. A retry starts the handler from the top,
so ordinary code outside Steps runs again.

To observe an email retry, start the onboarding worker with:

```sh
EMAIL_FAIL_ONCE=true go run ./examples/onboarding
```

Invoke a new Run. The simulated email service fails its first delivery attempt
for each welcome-email key. The account Step returns its recorded result on
retry, and email delivery is tried again. Once email retries are exhausted,
the Run fails and the account remains created.

Recording happens **after** the service call succeeds. If a process dies
between those two actions, that call may run again. Use stable business IDs
and enforce uniqueness in your storage and delivery provider. This example
deduplicates accounts and welcome emails in memory only; those records disappear
on restart and are not shared by worker processes. Steps preserve progress;
they do not make external operations transactional.

A plain error is retryable. `runcoil.Permanent(err)` fails immediately;
`runcoil.RetryAfter(err, delay)` changes the next retry delay. Set a policy with
`runcoil.Retry` on a Task or `step.Retry` on a Step. `MaxAttempts` includes the
first try, and the last value in `Delays` repeats. A Step returns `*step.Error`
once its failure is final; before that, the SDK stops the attempt for retry.

### Handler execution rules

Call `step.Do` on the handler's own goroutine. The SDK stops an attempt with
a panic recovered by the worker; application recovery code must re-panic
values it does not recognize. Step functions must return promptly when their
context is cancelled. A timeout you set on a derived context is an ordinary
Step failure.

Outside a worker handler, `step.Do` calls the function directly and records
nothing. This lets you test application behavior without a server; it does
not simulate retry or replay.

## Worker configuration

A Task is identified by its worker ID and Task ID. Processes running the same
program use the same worker ID, and each Run goes to one of them. Different
programs need different IDs: registration replaces the Tasks and schedules
for that worker ID.

`runcoil.Slots(n)` controls concurrent Runs; the default is 10. Service
dependencies must support concurrent calls.

Give `runcoil.Logger` a `*slog.Logger` to enable SDK logs. The worker retries
server connections automatically and logs loss and restoration; running
handlers continue through an outage.

## Scheduled work

The [scheduled report example](examples/schedule/main.go) builds a report
through a report service and sends it through an email service. It runs daily
at 09:00 UTC with `SkipOverlap`. The reporting day comes from the original
scheduled tick, so retries after midnight keep the same day. The simulation
contains two sales from yesterday and deduplicates report delivery per day
within one process. For a quick local demonstration:

```sh
REPORT_INTERVAL=3s go run ./examples/schedule
```

`schedule.Daily`, `schedule.Cron`, and `schedule.Every` describe schedules in
UTC. A Task has at most one schedule and receives no input on a scheduled Run.
`schedule.SkipOverlap()` skips a tick while the previous Run is unfinished.
`runcoil.StartDeadline(d)` fails a Run that cannot start within `d` of its due
time. Daily schedules are not accelerated unless `REPORT_INTERVAL` is set.

The small [hello example](examples/hello/main.go) is also retained for the
server's quick start.

## Reference and development

- [runcoil](https://pkg.go.dev/github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil): client, worker, handlers, and Task options.
- [step](https://pkg.go.dev/github.com/tuanvumaihuynh/runcoil-go/pkg/step): recorded calls and Step retry options.
- [schedule](https://pkg.go.dev/github.com/tuanvumaihuynh/runcoil-go/pkg/schedule): daily, cron, and interval schedules.

Tools run through `go run` at pinned versions:

```sh
make test       # go test -race ./...
make test-cov   # coverage report in bin/coverage.html
make lint       # golangci-lint v2
```

Build and test this repository independently; no sibling checkout is required.
