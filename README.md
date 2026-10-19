# runcoil-go

The Go SDK for [Runcoil](https://github.com/tuanvumaihuynh/runcoil), a
self-hosted server for background tasks. Run handlers in your application,
record progress with Steps, and schedule recurring work in UTC.

**Status: pre-release.** The API may change before version 1.0. This SDK runs
workers; callers invoke Tasks through the server API.

## Installation

Requires Go 1.26 or newer. Until a version is tagged:

```sh
go get github.com/tuanvumaihuynh/runcoil-go@main
```

## Usage

Wrap application service calls in `step.Do`. This excerpt from the
[onboarding example](examples/onboarding/) creates an account and sends a
welcome email:

```go
func (o *onboarding) createAccount(ctx context.Context, in onboardingInput) (onboardingOutput, error) {
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
		return onboardingOutput{}, err
	}

	return onboardingOutput{AccountID: created.ID, MessageID: messageID}, nil
}
```

On retry, Steps with recorded successes return their stored results. Service
calls still need idempotency if a process dies before the result is recorded.

## Start and shut down

Pass `Start` a context that ends on Ctrl-C or SIGTERM. When the signal arrives,
shut the worker down with a drain context:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := worker.Start(ctx); err != nil {
	return err
}

<-ctx.Done()

drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

return worker.Shutdown(drainCtx)
```

When registration fails, `Start` returns the error, so a process that cannot
reach the server exits and its supervisor can restart it. Once started, the
worker reconnects with backoff while the server is unavailable, for example
while it restarts. `Shutdown`
closes the Listen stream and waits for running handlers. When the drain context
ends first, it cancels the handlers still running and reports them as released.

## Examples

See [examples](examples/) for complete programs, setup, run commands, and retry
walkthroughs:

- [Onboarding](examples/onboarding/): account creation and welcome email.
- [Scheduled report](examples/schedule/): daily sales report and email delivery.
- [Hello](examples/hello/): a minimal worker.

## Documentation

API reference: [runcoil](https://pkg.go.dev/github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil),
[step](https://pkg.go.dev/github.com/tuanvumaihuynh/runcoil-go/pkg/step), and
[schedule](https://pkg.go.dev/github.com/tuanvumaihuynh/runcoil-go/pkg/schedule).

See the [server repository](https://github.com/tuanvumaihuynh/runcoil) for server
setup and its API.

## Development

Use `make test` for race-enabled tests and `make lint` for lint checks. Tools
run at pinned versions through `go run`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
