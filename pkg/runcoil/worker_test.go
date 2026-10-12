package runcoil_test

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/schedule"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)

// A result that is being reported when the drain deadline passes is not cut
// short: it reaches the server within the release budget.
func TestFinalReportInFlightAtDrainDeadlineIsDelivered(t *testing.T) {
	f := newFakeServer()
	f.update(func(f *fakeServer) { f.finishOutage = 2 })
	h := newHarness(t, f)
	w := h.worker(t)
	returned := make(chan struct{})
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(context.Context, struct{}) (string, error) {
		close(returned)
		return "ok", nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_fr1", "tok_fr1", "order", ""))
	<-returned
	time.Sleep(100 * time.Millisecond) // the first FinishRun has failed; the report is in its retry

	drain, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, w.Shutdown(drain), "shutdown")

	fin := f.nextFinish(t)
	require.Equal(t, "run_fr1", fin.GetRunId())
	require.NotNil(t, fin.GetSucceeded(), "finish = run %q result %v, want succeeded run_fr1",
		fin.GetRunId(), fin.GetResult())
}

// Release is sent only after a handler that is slow to notice the cancellation
// has returned, so the Run cannot be picked up while its Step still runs.
func TestReleaseWaitsForSlowHandlerToReturn(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	started := make(chan struct{})
	returned := make(chan struct{}, 1)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(context.Context, struct{}) (string, error) {
		close(started)
		time.Sleep(500 * time.Millisecond) // ignores its context for a while
		returned <- struct{}{}
		return "late", nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_sl1", "tok_sl1", "order", ""))
	<-started

	drain, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, w.Shutdown(drain), "shutdown")

	fin := f.nextFinish(t)
	require.Equal(t, "run_sl1", fin.GetRunId())
	require.NotNil(t, fin.GetReleased(), "finish = run %q result %v, want released run_sl1",
		fin.GetRunId(), fin.GetResult())

	select {
	case <-returned:
	default:
		require.Fail(t, "release was sent before the handler returned")
	}
}

// An attempt named in HeartbeatResponse.stale stops without any report.
func TestStaleHeartbeatStopsHandlerWithoutReport(t *testing.T) {
	f := newFakeServer()
	f.update(func(f *fakeServer) { f.heartbeatStale["run_st2"] = true })
	h := newHarness(t, f)
	w := h.worker(t)
	started := make(chan struct{})
	returned := make(chan struct{})
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		close(started)
		<-ctx.Done()
		close(returned)
		return struct{}{}, ctx.Err()
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_st2", "tok_st2", "order", ""))
	<-started
	<-returned
	f.expectNoFinish(t, 300*time.Millisecond)
}

// The server drops the stream: the worker reconnects and keeps the handler it holds.
func TestStreamDropReconnectsAndKeepsRunning(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	started := make(chan struct{})
	gate := make(chan struct{})
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(context.Context, struct{}) (string, error) {
		close(started)
		<-gate
		return "kept", nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_k1", "tok_k1", "order", ""))
	<-started
	listens := f.listenCount()
	f.dropStream()
	waitFor(t, "a new Listen stream", func() bool { return f.listenCount() > listens })
	close(gate)

	fin := f.nextFinish(t)
	require.Equal(t, "run_k1", fin.GetRunId())
	require.NotNil(t, fin.GetSucceeded(), "finish = run %q result %v, want succeeded run_k1",
		fin.GetRunId(), fin.GetResult())

	require.Equal(t, 1, f.registerCount(), "register calls: a dropped stream does not register again")
}

// A heartbeat names the Step whose function runs now, and an empty Step between Steps.
func TestHeartbeatNamesRunningStep(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	inSecond := make(chan struct{})
	gate := make(chan struct{})
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		if _, err := step.Do(ctx, "first", func(context.Context) (string, error) { return "1", nil }); err != nil {
			return struct{}{}, err
		}

		if _, err := step.Do(ctx, "second", func(context.Context) (string, error) {
			close(inSecond)
			<-gate
			return "2", nil
		}); err != nil {
			return struct{}{}, err
		}

		time.Sleep(300 * time.Millisecond) // between Steps: no Step function runs
		_, err := step.Do(ctx, "third", func(context.Context) (string, error) { return "3", nil })
		return struct{}{}, err
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_hb1", "tok_hb1", "order", ""))
	<-inSecond
	waitFor(t, "a heartbeat naming second", func() bool {
		return containsString(f.heartbeatSteps("run_hb1"), "second")
	})

	close(gate)

	f.nextFinish(t)
	seq := f.heartbeatSteps("run_hb1")
	at := indexOfString(seq, "second")
	require.GreaterOrEqual(t, at, 0, "no heartbeat named second: %v", seq)

	require.Contains(t, seq[at+1:], "", "no empty heartbeat between second and third: %v", seq)

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, refs := range f.heartbeats {
		for _, r := range refs {
			if r.GetStepId() == "second" {
				require.EqualValues(t, 1, r.GetStepAttempt(), "heartbeat step attempt")
			}
		}
	}
}

func containsString(list []string, want string) bool {
	return indexOfString(list, want) >= 0
}

func indexOfString(list []string, want string) int {
	for i, s := range list {
		if s == want {
			return i
		}
	}

	return -1
}

// heartbeatOrder is a handler with two Steps: "charge", which has a retry policy
// of one attempt, and "refund", which has none. Each Step waits on its gate.
func heartbeatOrder(inCharge, gateCharge, gateRefund chan struct{}) runcoil.Handler {
	return runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		if _, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
			close(inCharge)
			<-gateCharge
			return "1", nil
		}, step.Retry(step.RetryPolicy{MaxAttempts: 1})); err != nil {
			return struct{}{}, err
		}

		time.Sleep(300 * time.Millisecond) // between Steps
		_, err := step.Do(ctx, "refund", func(context.Context) (string, error) {
			<-gateRefund
			return "2", nil
		})

		return struct{}{}, err
	})
}

// assertChargeTryOne checks the heartbeat that names charge: try 1 under a policy
// of one attempt and no delay.
func assertChargeTryOne(t *testing.T, f *fakeServer, runID string) {
	t.Helper()

	for _, r := range f.heartbeatRefsOf(runID) {
		if r.GetStepId() == "charge" {
			require.EqualValues(t, 1, r.GetStepAttempt(),
				"charge heartbeat = step %d retry %v, want try 1 with max attempts 1", r.GetStepAttempt(), r.GetRetry())
			require.EqualValues(t, 1, r.GetRetry().GetMaxAttempts(), "charge heartbeat retry %v", r.GetRetry())
			require.Empty(t, r.GetRetry().GetDelays(), "charge heartbeat retry %v", r.GetRetry())

			break
		}
	}
}

// emptyHeartbeatAfterCharge reports whether a heartbeat with no Step follows one
// that names charge: the state between the two Steps.
func emptyHeartbeatAfterCharge(f *fakeServer, runID string) bool {
	seen := false
	for _, r := range f.heartbeatRefsOf(runID) {
		if r.GetStepId() == "charge" {
			seen = true
		}

		if seen && r.GetStepId() == "" && r.GetRetry() == nil && r.GetStepAttempt() == 0 {
			return true
		}
	}

	return false
}

// namesStep reports whether a heartbeat for the run names the Step.
func namesStep(f *fakeServer, runID, stepID string) bool {
	for _, r := range f.heartbeatRefsOf(runID) {
		if r.GetStepId() == stepID {
			return true
		}
	}

	return false
}

// A heartbeat carries the running Step's own policy, and none for a Step without
// one. Between Steps all three fields are empty.
func TestHeartbeatCarriesStepOwnPolicy(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	inCharge := make(chan struct{})
	gateCharge := make(chan struct{})
	gateRefund := make(chan struct{})
	require.NoError(t, w.NewTask("order", heartbeatOrder(inCharge, gateCharge, gateRefund)))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_pol1", "tok_pol1", "order", ""))
	<-inCharge
	waitFor(t, "a heartbeat naming charge with its policy", func() bool {
		for _, r := range f.heartbeatRefsOf("run_pol1") {
			if r.GetStepId() == "charge" && r.GetRetry() != nil {
				return true
			}
		}

		return false
	})

	assertChargeTryOne(t, f, "run_pol1")

	close(gateCharge)

	// Between the two Steps: no Step function runs, so every field is empty.
	waitFor(t, "an empty heartbeat between the Steps", func() bool {
		return emptyHeartbeatAfterCharge(f, "run_pol1")
	})

	waitFor(t, "a heartbeat naming refund without a policy", func() bool {
		return namesStep(f, "run_pol1", "refund")
	})

	for _, r := range f.heartbeatRefsOf("run_pol1") {
		if r.GetStepId() == "refund" {
			require.Nil(t, r.GetRetry(), "refund heartbeat carries a policy; the Step has none")
		}
	}

	close(gateRefund)
	require.NotNil(t, f.nextFinish(t).GetSucceeded(), "run did not succeed")
}

type logEntry struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

// capture is a slog.Handler that keeps every record, for the tests to read.
type capture struct {
	mu      sync.Mutex
	entries []logEntry
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(_ context.Context, r slog.Record) error {
	e := logEntry{level: r.Level, msg: r.Message, attrs: map[string]any{}}
	r.Attrs(func(a slog.Attr) bool {
		e.attrs[a.Key] = a.Value.Any()
		return true
	})

	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, e)
	return nil
}

func (c *capture) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *capture) WithGroup(string) slog.Handler { return c }

// count returns how many records at level have the message msg.
func (c *capture) count(level slog.Level, msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.entries {
		if e.level == level && e.msg == msg {
			n++
		}
	}

	return n
}

// countLevel returns how many records have the given level.
func (c *capture) countLevel(level slog.Level) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.entries {
		if e.level == level {
			n++
		}
	}

	return n
}

// first returns the first record with the message msg.
func (c *capture) first(msg string) (logEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if e.msg == msg {
			return e, true
		}
	}

	return logEntry{}, false
}

// An outage is one warning, then debug lines, then a reminder on its interval;
// the first answer after it is one info line. A second outage logs again.
func TestOutageLogsOnceRemindsAndRestores(t *testing.T) {
	restore := runcoil.SetReminderEvery(700 * time.Millisecond)
	t.Cleanup(restore)

	f := newFakeServer()
	h := newHarness(t, f)
	logs := &capture{}
	w := h.workerFor(t, h.clientWith(t, runcoil.Logger(slog.New(logs))), "demo")
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	})))

	h.start(t, w)

	// First outage: many failed attempts, one warning, then reminders.
	h.setDown(true)
	assertFirstOutageLogs(t, logs)

	// Recovery: one info line, and the reminders stop.
	h.setDown(false)
	waitFor(t, "the restored line", func() bool {
		return logs.count(slog.LevelInfo, "server connection restored") == 1
	})

	reminders := logs.count(slog.LevelWarn, "server still unreachable")
	time.Sleep(1500 * time.Millisecond)
	n := logs.count(slog.LevelWarn, "server still unreachable")
	require.Equal(t, reminders, n, "reminders kept coming after recovery")

	// Second outage: logged again.
	h.setDown(true)
	waitFor(t, "the second lost warning", func() bool {
		return logs.count(slog.LevelWarn, "server connection lost") == 2
	})

	h.setDown(false)
	waitFor(t, "the second restored line", func() bool {
		return logs.count(slog.LevelInfo, "server connection restored") == 2
	})
}

// assertFirstOutageLogs waits for the first outage to log its failed attempts, its
// one warning, and its reminder, and checks the attributes of the warning and the reminder.
func assertFirstOutageLogs(t *testing.T, logs *capture) {
	t.Helper()

	waitFor(t, "several failed attempts", func() bool {
		return logs.count(slog.LevelDebug, "server connection attempt failed") >= 3
	})
	require.Equal(t, 1, logs.count(slog.LevelWarn, "server connection lost"), "lost warnings")

	require.Equal(t, 1, logs.countLevel(slog.LevelWarn), "warnings before the reminder, want only the lost warning")

	lost, _ := logs.first("server connection lost")
	require.Equal(t, "demo", lost.attrs["worker_id"], "lost warning attributes = %v", lost.attrs)
	require.NotEmpty(t, lost.attrs["instance_id"], "lost warning attributes = %v", lost.attrs)
	require.NotNil(t, lost.attrs["error"], "lost warning attributes = %v", lost.attrs)

	// The reminder repeats the warning while the outage lasts.
	waitFor(t, "the reminder", func() bool {
		return logs.count(slog.LevelWarn, "server still unreachable") >= 1
	})
	rem, _ := logs.first("server still unreachable")
	require.NotNil(t, rem.attrs["attempts"], "reminder attributes = %v", rem.attrs)
	require.NotNil(t, rem.attrs["down_for"], "reminder attributes = %v", rem.attrs)
}

// NOT_REGISTERED followed by a successful register is one info line, not a warning.
func TestNotRegisteredRegistersAgainWithoutWarning(t *testing.T) {
	f := newFakeServer()
	f.update(func(f *fakeServer) { f.notRegistered = 1 })
	h := newHarness(t, f)
	logs := &capture{}
	w := h.workerFor(t, h.clientWith(t, runcoil.Logger(slog.New(logs))), "demo")
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	})))

	h.start(t, w)

	waitFor(t, "the registered-again line", func() bool {
		return logs.count(slog.LevelInfo, "worker registered again") == 1
	})

	time.Sleep(200 * time.Millisecond)
	require.Zero(t, logs.countLevel(slog.LevelWarn), "warnings, want none for a re-registration")

	require.Zero(t, logs.count(slog.LevelWarn, "server connection lost"),
		"server connection lost logged for a server that answered")
}

// After a successful re-register the worker opens Listen at once, even when
// earlier failures had grown the reconnect backoff. Heartbeats fail in this
// test, so only the re-registration can end the wait.
func TestListenOpensAtOnceAfterReregister(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	})))

	h.start(t, w)

	// Failed attempts at about 0.5s and 1.5s grow the backoff to 2s.
	h.setDown(true)
	time.Sleep(2500 * time.Millisecond)
	f.update(func(f *fakeServer) {
		f.notRegistered = 1
		f.heartbeatDown = true
	})

	h.setDown(false)

	waitFor(t, "a re-register", func() bool { return f.registerCount() >= 2 })
	regAt := f.lastRegisterTime()
	waitFor(t, "a Listen after the re-register", func() bool {
		_, ok := f.listenAfter(regAt)
		return ok
	})

	at, _ := f.listenAfter(regAt)
	f.update(func(f *fakeServer) { f.heartbeatDown = false })
	gap := at.Sub(regAt)
	require.LessOrEqual(t, gap, 200*time.Millisecond,
		"Listen opened after the re-register; want at once, not after the backoff")
}

// A server answer during the backoff ends the wait: the next Listen does not wait
// for the rest of the backoff.
func TestServerAnswerEndsBackoffWait(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	})))

	h.start(t, w)

	h.setDown(true)
	time.Sleep(2500 * time.Millisecond) // the next attempt would be 1s away
	back := time.Now()
	h.setDown(false)
	waitFor(t, "a Listen after the server answered", func() bool {
		_, ok := f.listenAfter(back)
		return ok
	})

	at, _ := f.listenAfter(back)
	gap := at.Sub(back)
	require.LessOrEqual(t, gap, 500*time.Millisecond,
		"Listen opened after the server answered; the backoff was not reset")
}

func TestNewWorkerRequiresClientAndWorkerID(t *testing.T) {
	client, err := runcoil.NewClient(runcoil.Endpoint("http://localhost:7070"), runcoil.Token("t"))
	require.NoError(t, err)

	_, err = runcoil.NewWorker(nil, runcoil.WorkerID("demo"))
	require.Error(t, err, "NewWorker accepted a nil client")

	_, err = runcoil.NewWorker(&runcoil.Client{}, runcoil.WorkerID("demo"))
	require.Error(t, err, "NewWorker accepted a client that did not come from NewClient")

	_, err = runcoil.NewWorker(client)
	require.EqualError(t, err, "new worker: worker id is required", "NewWorker accepted no WorkerID")

	_, err = runcoil.NewWorker(client, runcoil.WorkerID(""))
	require.EqualError(t, err, "new worker: worker id is required", "NewWorker accepted an empty worker ID")

	_, err = runcoil.NewWorker(client, runcoil.WorkerID("demo"))
	require.NoError(t, err)
}

// Register carries the worker ID that WorkerID gives.
func TestRegisterCarriesWorkerID(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.workerFor(t, h.client(t), "orders")

	h.start(t, w)

	require.Equal(t, "orders", f.registerRequests()[0].GetWorkerId())
}

// When WorkerID is given twice, the last one wins, as for any functional option.
// workerFor passes its worker ID first, so the WorkerID in opts is the later one.
func TestLastWorkerIDWins(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.workerFor(t, h.client(t), "first", runcoil.WorkerID("second"))

	h.start(t, w)

	require.Equal(t, "second", f.registerRequests()[0].GetWorkerId())
}

func TestSlotsBelowOneFailsNewWorker(t *testing.T) {
	client, err := runcoil.NewClient(runcoil.Endpoint("http://localhost:7070"), runcoil.Token("t"))
	require.NoError(t, err)

	for _, n := range []int{0, -1} {
		_, err = runcoil.NewWorker(client, runcoil.WorkerID("demo"), runcoil.Slots(n))
		require.Error(t, err, "NewWorker accepted Slots(%d)", n)
	}

	_, err = runcoil.NewWorker(client, runcoil.WorkerID("demo"), runcoil.Slots(1))
	require.NoError(t, err, "NewWorker rejected Slots(1)")
}

type helloIn struct {
	Name string `json:"name"`
}

type helloOut struct {
	Greeting string `json:"greeting"`
}

type modeIn struct {
	Mode string `json:"mode"`
}

func TestRunSucceedsWithJSONOutput(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, in helloIn) (helloOut, error) {
		return helloOut{Greeting: "hello, " + in.Name}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_1", "tok_1", "hello", `{"name":"ada"}`))
	fin := f.nextFinish(t)

	require.Equal(t, "run_1", fin.GetRunId())
	require.Equal(t, "tok_1", fin.GetLeaseToken())

	succeeded := fin.GetSucceeded()
	require.NotNil(t, succeeded, "result = %v, want succeeded", fin.GetResult())

	assertJSON(t, succeeded.GetOutput(), `{"greeting":"hello, ada"}`)

	require.Equal(t, 1, f.ackCount())

	require.Zero(t, f.badAuthCount(), "calls without the bearer token")

	f.mu.Lock()
	reg := f.registers[0]
	f.mu.Unlock()
	require.Equal(t, "demo", reg.GetWorkerId())
	require.Len(t, reg.GetTasks(), 1, "register = %v", reg)
	require.Equal(t, "hello", reg.GetTasks()[0].GetTaskId())

	require.EqualValues(t, 1, reg.GetProtocolVersion())
	require.Equal(t, "runcoil-go", reg.GetSdkName())
	require.NotEmpty(t, reg.GetInstanceId(), "register metadata = %v", reg)
}

func TestNotRegisteredRegistersAgainThenListens(t *testing.T) {
	f := newFakeServer()
	f.notRegistered = 1
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, in helloIn) (helloOut, error) {
		return helloOut{Greeting: "hello, " + in.Name}, nil
	})))

	h.start(t, w)

	waitFor(t, "a second register and listen", func() bool {
		return f.registerCount() >= 2 && f.listenCount() >= 2
	})

	f.dispatch(t, testDispatch("run_6", "tok_6", "hello", `{"name":"grace"}`))
	succeeded := f.nextFinish(t).GetSucceeded()
	require.NotNil(t, succeeded, "run did not succeed after the worker registered again")

	assertJSON(t, succeeded.GetOutput(), `{"greeting":"hello, grace"}`)
}

func TestShutdownReleasesRunPastDrainDeadline(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)

	started := make(chan struct{})
	returned := make(chan error, 1)
	require.NoError(t, w.NewTask("slow", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		close(started)
		<-ctx.Done()
		returned <- ctx.Err()
		return struct{}{}, ctx.Err()
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_7", "tok_7", "slow", ""))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		require.Fail(t, "handler did not start")
	}

	waitFor(t, "a heartbeat that names the run", func() bool { return f.heartbeatNamed("run_7") })

	drain, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, w.Shutdown(drain), "shutdown")

	fin := f.nextFinish(t)
	require.Equal(t, "run_7", fin.GetRunId())
	require.Equal(t, "tok_7", fin.GetLeaseToken())
	require.NotNil(t, fin.GetReleased(), "finish = run %q result %v, want released run_7",
		fin.GetRunId(), fin.GetResult())

	select {
	case err := <-returned:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		require.Fail(t, "handler was not cancelled")
	}

	// The handler returned after the release: it must not report a second result.
	f.expectNoFinish(t, 200*time.Millisecond)
}

func TestTwoWorkersOnOneClientHaveTheirOwnInstances(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	c := h.client(t)
	wa := h.workerFor(t, c, "alpha")
	wb := h.workerFor(t, c, "beta")
	require.NoError(t, wa.NewTask("ta", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (string, error) {
		return "a", nil
	})))

	require.NoError(t, wb.NewTask("tb", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (string, error) {
		return "b", nil
	})))

	h.start(t, wa)
	h.start(t, wb)

	ia := f.instanceOf(t, "alpha")
	ib := f.instanceOf(t, "beta")
	require.NotEmpty(t, ia, "instance ID of alpha")
	require.NotEqual(t, ia, ib, "want two different instance IDs")

	f.dispatchTo(t, ia, testDispatch("run_a", "tok_a", "ta", ""))
	f.dispatchTo(t, ib, testDispatch("run_b", "tok_b", "tb", ""))

	got := map[string]string{}
	for range 2 {
		fin := f.nextFinish(t)
		require.NotNil(t, fin.GetSucceeded(), "run %s result = %v, want succeeded", fin.GetRunId(), fin.GetResult())

		got[fin.GetRunId()] = string(fin.GetSucceeded().GetOutput())
	}

	require.Equal(t, `"a"`, got["run_a"])
	require.Equal(t, `"b"`, got["run_b"])
}

func TestStartReturnsRegisterError(t *testing.T) {
	f := newFakeServer()
	f.update(func(f *fakeServer) { f.registerErr = errors.New("no such worker ID") })
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})))

	require.Error(t, w.Start(context.Background()), "Start succeeded although Register failed")
}

func TestRegisterSendsPoliciesAndDeadline(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}), runcoil.StartDeadline(time.Minute), runcoil.Retry(runcoil.RetryPolicy{MaxAttempts: 3, Delays: []time.Duration{time.Second}})))

	h.start(t, w)

	def := f.registerRequests()[0].GetTasks()[0]
	require.Equal(t, time.Minute, def.GetStartDeadline().AsDuration())

	require.EqualValues(t, 3, def.GetRetry().GetMaxAttempts())
	require.Len(t, def.GetRetry().GetDelays(), 1, "retry = %v", def.GetRetry())
	require.Equal(t, time.Second, def.GetRetry().GetDelays()[0].AsDuration())

	require.Nil(t, def.GetSchedule(), "want no schedule")
}

func TestRegisterSendsSchedule(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("report", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	}), runcoil.Schedule(schedule.Cron("0 9 * * *", schedule.SkipOverlap()))))

	h.start(t, w)

	sched := f.registerRequests()[0].GetTasks()[0].GetSchedule()
	require.Equal(t, "0 9 * * *", sched.GetCron())
	require.True(t, sched.GetSkipOverlap(), "schedule = %v, want cron with skip overlap", sched)
}

func TestRegisterSendsEveryAndDailySchedules(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	noop := runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (struct{}, error) { return struct{}{}, nil })
	require.NoError(t, w.NewTask("every", noop, runcoil.Schedule(schedule.Every(3*time.Second, schedule.SkipOverlap()))))

	require.NoError(t, w.NewTask("daily", noop, runcoil.Schedule(schedule.Daily(schedule.At(9, 0), schedule.At(17, 30)))))

	h.start(t, w)

	tasks := f.registerRequests()[0].GetTasks()
	every := tasks[0].GetSchedule()
	require.Equal(t, 3*time.Second, every.GetEvery().AsDuration())
	require.True(t, every.GetSkipOverlap(), "every schedule = %v", every)

	daily := tasks[1].GetSchedule().GetDaily().GetAt()
	require.Len(t, daily, 2, "daily schedule = %v", tasks[1].GetSchedule())
	require.EqualValues(t, 9, daily[0].GetHour())
	require.EqualValues(t, 17, daily[1].GetHour())
	require.EqualValues(t, 30, daily[1].GetMinute())
}

func TestSlotsSetsRegisteredMaxConcurrentAttempts(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.workerFor(t, h.client(t), "demo", runcoil.Slots(3))
	h.start(t, w)

	require.EqualValues(t, 3, f.registerRequests()[0].GetMaxConcurrentAttempts())
}

func TestDefaultSlotsIsTen(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	h.start(t, w)

	require.EqualValues(t, 10, f.registerRequests()[0].GetMaxConcurrentAttempts(), "want the default 10")
}

// Slots(1) lets one handler run at a time. The second Run is acknowledged while
// it waits for the slot, and its handler starts only after the first returns.
func TestSlotsLimitsConcurrentHandlers(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.workerFor(t, h.client(t), "demo", runcoil.Slots(1))

	var running, peak atomic.Int32
	entered := make(chan string, 2)
	release := make(chan struct{})
	releaseHandlers := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseHandlers) // runs before the worker's shutdown, so a failed test does not wait for the drain
	require.NoError(t, w.NewTask("hold", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		now := running.Add(1)
		defer running.Add(-1)
		for {
			p := peak.Load()
			if now <= p || peak.CompareAndSwap(p, now) {
				break
			}
		}

		entered <- runcoil.Info(ctx).RunID
		<-release
		return struct{}{}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_s1", "tok_s1", "hold", ""))
	f.dispatch(t, testDispatch("run_s2", "tok_s2", "hold", ""))

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		require.Fail(t, "the first handler did not start")
	}

	waitFor(t, "the second acknowledgement", func() bool { return f.ackCount() == 2 })
	select {
	case id := <-entered:
		require.Failf(t, "run started while the only slot was taken", "run %s", id)
	case <-time.After(200 * time.Millisecond):
	}

	releaseHandlers()
	for range 2 {
		fin := f.nextFinish(t)
		require.NotNil(t, fin.GetSucceeded(), "run %s result = %v, want succeeded", fin.GetRunId(), fin.GetResult())
	}

	require.EqualValues(t, 1, peak.Load(), "peak number of handlers running at once")
}

// NewWorker rejects a Slots value that the uint32 field of the registration
// cannot hold. The check needs an int wider than 32 bits.
func TestSlotsAboveMaxUint32FailsNewWorker(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("int is 32 bits on this platform, so no Slots value is above math.MaxUint32")
	}

	client, err := runcoil.NewClient(runcoil.Endpoint("http://localhost:7070"), runcoil.Token("t"))
	require.NoError(t, err)

	var big uint64 = math.MaxUint32
	big++
	tooMany := int(big) //nolint:gosec // the test runs only where int is 64 bits
	_, err = runcoil.NewWorker(client, runcoil.WorkerID("demo"), runcoil.Slots(tooMany))
	require.Error(t, err, "NewWorker accepted Slots(%d)", tooMany)
}
