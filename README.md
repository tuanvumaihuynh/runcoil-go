# runcoil-go

The Go SDK for [Runcoil](https://github.com/tuanvumaihuynh/runcoil). A worker
built with it registers Tasks with a Runcoil server, receives their Runs, runs
the handlers in your own process, and reports each result.

**Status: pre-release.** The API may change before version 1.0. The SDK covers
the worker side; a caller invokes a Task through the server's API.

```go
client, err := runcoil.NewClient(
	runcoil.Endpoint("http://localhost:7070"),
	runcoil.Token("dev-token"),
	runcoil.Logger(slog.Default()), // without it the SDK logs nothing
)

worker, err := runcoil.NewWorker(client, runcoil.WorkerID("hello"))

err = worker.NewTask("hello", runcoil.HandlerFunc(hello))

err = worker.Start(ctx)
// ...
err = worker.Shutdown(drainCtx)
```

A handler has the shape `func(ctx context.Context, in In) (Out, error)`. The
input and the output are JSON.

```go
func hello(ctx context.Context, in helloInput) (helloOutput, error) {
	greeting, err := step.Do(ctx, "greet", func(context.Context) (string, error) {
		return "hello, " + in.Name, nil
	})
	if err != nil {
		return helloOutput{}, err
	}

	return helloOutput{Greeting: greeting}, nil
}
```

## Install

Use Go 1.26 or newer. Until a version is tagged, resolve the current source
revision from `main`:

```sh
go get github.com/tuanvumaihuynh/runcoil-go@main
```

The packages live under `pkg/`:

```go
import (
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/schedule"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)
```

Go records the resolved source revision in your module. The SDK pins its
schema dependency; no separate schema checkout is needed.

## Run the examples

Start a [Runcoil server](https://github.com/tuanvumaihuynh/runcoil#quick-start)
first. From a checkout of this repository:

```sh
go run ./examples/hello      # worker ID "hello": one Task with one Step
go run ./examples/order      # worker ID "orders": Steps, retries, and a compensation
go run ./examples/schedule   # worker ID "reports": a Task that runs every 3 seconds
```

Each example reads `RUNCOIL_ENDPOINT` (default `http://localhost:7070`) and
`RUNCOIL_TOKEN` (default `dev-token`). The server's README shows how to invoke
a Task and read its Run.

## Worker ID

`runcoil.WorkerID` names the worker. Every process that runs the same Tasks
uses the same worker ID, and a Run goes to one of them. A Task is identified by
its worker ID together with its Task ID.

One worker ID belongs to one program. A registration replaces the Tasks and
schedules stored for the worker ID, so two different programs must not share
one: the second would remove the first one's schedules.

`runcoil.Slots(n)` sets how many Runs a worker runs at once. The default is 10.

## Steps

`step.Do` records the outcome of a call with a side effect. A Run that is
retried replays the Steps that succeeded and does not run them again. A failed
Step is tried again by a later attempt. Once its tries are used up, `Do`
returns a `*step.Error`, and the handler can run a compensation.

- A plain error is retried. `runcoil.Permanent(err)` is not.
  `runcoil.RetryAfter(err, d)` sets the delay before the next try.
- `runcoil.Retry(policy)` on a Task and `step.Retry(policy)` on a Step replace
  the server's default policy: `MaxAttempts` and `Delays`.

A handler must follow two rules:

- Call `step.Do` on the handler's own goroutine. The SDK ends an attempt by
  panicking inside `Do`, and only the handler's goroutine recovers that panic.
- Code that recovers panics must re-panic any value it does not recognise. The
  SDK stops a handler this way when the server schedules a retry, and a
  `recover` that swallows the value lets the handler run on.

Once the server takes the attempt back, or a drain gives it back, `Do` stops
the handler and reports nothing. A timeout that the handler sets on its own
context is an ordinary failure of the Step. A Step function must return
promptly when its context is cancelled: a drain releases a Run only after its
handler has returned, or after two seconds.

## Schedules

```go
err = worker.NewTask("report", runcoil.HandlerFunc(report),
	runcoil.Schedule(schedule.Every(time.Hour, schedule.SkipOverlap())),
)
```

`schedule.Cron`, `schedule.Every`, and `schedule.Daily(schedule.At(9, 0))`
describe when a Task runs, in UTC. A Task has at most one schedule.
`schedule.SkipOverlap()` skips a tick while the previous Run is unfinished.
`runcoil.StartDeadline(d)` fails a Run that could not start within `d` of its
due time.

## Connection and logging

The SDK logs through the `*slog.Logger` given to `runcoil.Logger`. Without that
option it logs nothing.

When the server is unreachable, a worker logs one warning, `server connection
lost`, and then repeats `server still unreachable` every minute while the
outage lasts. Each failed attempt is a debug line. The worker keeps retrying on
its own and logs one info line, `server connection restored`, when the server
answers again. Handlers that are running keep running through the outage.

## Packages

| Package | Holds |
| --- | --- |
| `pkg/runcoil` | `Client`, `Worker`, `HandlerFunc`, Task options, `Permanent`, `RetryAfter`, `Info` |
| `pkg/step` | `Do`, `Retry`, `RetryPolicy`, `Error` |
| `pkg/schedule` | `Cron`, `Every`, `Daily`, `At`, `SkipOverlap` |

`step` imports `runcoil`, never the reverse. Shared state lives in
`internal/runstate`. `step.Error` is an alias of a type in that package, so the
worker can see it without importing `step`.

## Development

Tools run through `go run`, pinned by version; nothing is installed globally.

```sh
make test       # go test -race ./...
make test-cov   # coverage report in bin/coverage.html
make lint       # golangci-lint v2
```

The module pins a published source revision of
[runcoil-proto](https://github.com/tuanvumaihuynh/runcoil-proto) in `go.mod`.
Build and test this repository independently; no sibling checkout or
`go.work` is required.
