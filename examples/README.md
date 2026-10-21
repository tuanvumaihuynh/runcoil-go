# Examples

Runnable examples for the Runcoil Go SDK. The worker programs register a worker,
connect to the server, and drain running handlers on shutdown. The invoke program
calls a Task and waits for its Run.

- [Onboarding](onboarding/): create an account and send a welcome email through separate application services.
- [Invoke](invoke/): invoke the onboarding Task, wait for its Run, and print the result.
- [Scheduled report](schedule/): build yesterday's sales report and email it at 09:00 UTC.
- [Hello](hello/): the minimal worker used by the server quick start.

The services in onboarding and schedule simulate storage and email delivery.
Their state is in memory and disappears on restart. Full wiring and handlers
are in each example's `main.go`; simulated application services are in
`services.go`.

Commands below run from the repository root and require Go 1.26 or newer.

## Run it

Start a [Runcoil server](https://github.com/tuanvumaihuynh/runcoil#quick-start),
then run the worker from a checkout of this repository:

```sh
go run ./examples/onboarding
```

Examples read `RUNCOIL_ENDPOINT` (default `http://localhost:7070`) and
`RUNCOIL_TOKEN` (default `dev-token`). Stop with Ctrl-C to drain running
handlers for up to 30 seconds.

In another terminal, invoke worker `accounts`, Task `create-account`, and wait
for the result with the SDK:

```sh
go run ./examples/invoke
```

It prints the account and welcome email IDs when the Run succeeds. When the Run
fails, it prints the failure code, message, and Step.

Without the SDK, call the server API directly. Invoke the same Task with `curl`:

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

Invoke a new Run with a new idempotency key. The SDK example uses the key
`onboarding/acct-ada`, which returns the original Run; change that key in
`examples/invoke/main.go`, or use a new key with the `curl` commands. The
simulated email service fails its first delivery attempt for each welcome-email
key. The account Step returns its recorded result on
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

The [scheduled report example](schedule/) builds a report
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

The small [hello example](hello/) is also retained for the
server's quick start.

