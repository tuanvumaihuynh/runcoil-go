package runcoil_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/schedule"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewTaskValidates(t *testing.T) {
	client, err := runcoil.NewClient(runcoil.Endpoint("http://127.0.0.1:1"), runcoil.Token("t"))
	require.NoError(t, err)

	w, err := runcoil.NewWorker(client, runcoil.WorkerID("demo"))
	require.NoError(t, err)

	fn := runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil })

	require.Error(t, w.NewTask("", fn), "empty task id accepted")

	require.Error(t, w.NewTask("nil-handler", runcoil.Handler{}), "zero handler accepted")

	require.Error(t, w.NewTask("zero-schedule", fn, runcoil.Schedule(schedule.Schedule{})), "zero schedule accepted")

	require.Error(t, w.NewTask("bad-every", fn, runcoil.Schedule(schedule.Every(0))), "non-positive every accepted")

	require.Error(t, w.NewTask("bad-hour", fn, runcoil.Schedule(schedule.Daily(schedule.At(24, 0)))), "hour 24 accepted")

	require.Error(t, w.NewTask("bad-minute", fn, runcoil.Schedule(schedule.Daily(schedule.At(9, 60)))), "minute 60 accepted")

	require.Error(t, w.NewTask("empty-cron", fn, runcoil.Schedule(schedule.Cron(""))), "empty cron accepted")

	require.Error(t, w.NewTask("neg-attempts", fn, runcoil.Retry(runcoil.RetryPolicy{MaxAttempts: -1})),
		"negative attempts accepted")

	require.ErrorContains(t, w.NewTask("zero-delay", fn, runcoil.Retry(runcoil.RetryPolicy{Delays: []time.Duration{0}})),
		"retry: delays must be greater than 0")

	require.NoError(t, w.NewTask("positive-delay", fn, runcoil.Retry(runcoil.RetryPolicy{Delays: []time.Duration{time.Nanosecond}})))

	require.Error(t, w.NewTask("neg-deadline", fn, runcoil.StartDeadline(-time.Second)), "negative start deadline accepted")

	require.NoError(t, w.NewTask("hello", fn, runcoil.Schedule(schedule.Daily(schedule.At(9, 0), schedule.At(17, 30)))))

	require.Error(t, w.NewTask("hello", fn), "duplicate task id accepted")
}

func TestPermanentAndRetryAfterKeepTheError(t *testing.T) {
	base := errors.New("base")
	require.Nil(t, runcoil.Permanent(nil), "markers of nil must be nil")
	require.Nil(t, runcoil.RetryAfter(nil, time.Second), "markers of nil must be nil")

	wrapped := runcoil.Permanent(base)
	require.ErrorIs(t, wrapped, base, "Permanent does not wrap its error")

	permanent, _ := runstate.Classify(wrapped)
	require.True(t, permanent, "Permanent error is not classified as permanent")

	after := runcoil.RetryAfter(base, time.Minute)
	require.ErrorIs(t, after, base, "RetryAfter does not wrap its error")

	permanent, delay := runstate.Classify(after)
	require.False(t, permanent, "RetryAfter error is classified as permanent")
	require.Equal(t, time.Minute, delay, "RetryAfter error delay")

	both := runcoil.Permanent(runcoil.RetryAfter(base, time.Minute))
	permanent, delay = runstate.Classify(both)
	require.True(t, permanent, "error with both markers is not classified as permanent")
	require.Equal(t, time.Minute, delay, "error with both markers delay")
}

func TestScheduleAndDeadlineRulesMatchTheServer(t *testing.T) {
	client, err := runcoil.NewClient(runcoil.Endpoint("http://127.0.0.1:1"), runcoil.Token("t"))
	require.NoError(t, err)

	w, err := runcoil.NewWorker(client, runcoil.WorkerID("demo"))
	require.NoError(t, err)

	fn := runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil })

	err = w.NewTask("sub-second", fn, runcoil.Schedule(schedule.Every(500*time.Millisecond)))
	require.Error(t, err, "an Every below 1 second was accepted")

	err = w.NewTask("tz-prefix", fn, runcoil.Schedule(schedule.Cron("TZ=Europe/Paris 0 9 * * *")))
	require.Error(t, err, "a TZ= prefix was accepted; schedules are UTC only")

	err = w.NewTask("cron-tz-prefix", fn, runcoil.Schedule(schedule.Cron("CRON_TZ=UTC 0 9 * * *")))
	require.Error(t, err, "a CRON_TZ= prefix was accepted")

	err = w.NewTask("one-second", fn, runcoil.Schedule(schedule.Every(time.Second)))
	require.NoError(t, err, "an Every of exactly 1 second was rejected")

	// Zero means no deadline; only a negative value is rejected.
	err = w.NewTask("no-deadline", fn, runcoil.StartDeadline(0))
	require.NoError(t, err, "a zero StartDeadline was rejected")
}

func TestInfoInsideHandlerCarriesRunState(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	infos := make(chan runcoil.RunInfo, 1)
	require.NoError(t, w.NewTask("report", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		infos <- runcoil.Info(ctx)
		return struct{}{}, nil
	})))

	h.start(t, w)

	due := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	d := testDispatch("run_12", "tok_12", "report", "")
	d.Attempt = 2
	d.Source = runcoilv1.RunSource_RUN_SOURCE_SCHEDULE
	d.DueAt = timestamppb.New(due)
	f.dispatch(t, d)

	got := <-infos
	require.Equal(t, "run_12", got.RunID, "Info = %+v", got)
	require.Equal(t, "demo", got.WorkerID, "Info = %+v", got)
	require.Equal(t, "report", got.TaskID, "Info = %+v", got)
	require.Equal(t, 2, got.Attempt, "Info = %+v", got)

	require.True(t, got.ScheduledAt.Equal(due), "ScheduledAt = %v, want %v", got.ScheduledAt, due)

	f.nextFinish(t)
}

// An empty input decodes to the zero value of the handler's input type, which
// is how a scheduled Run arrives: it carries no input.
func TestEmptyInputDecodesToZeroValue(t *testing.T) {
	type regionIn struct {
		Region string `json:"region"`
		Rows   int    `json:"rows"`
	}

	type regionOut struct {
		Zero bool `json:"zero"`
	}

	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("report", runcoil.HandlerFunc(func(_ context.Context, in regionIn) (regionOut, error) {
		return regionOut{Zero: in == regionIn{}}, nil
	})))

	h.start(t, w)

	d := testDispatch("run_z1", "tok_z1", "report", "")
	d.Source = runcoilv1.RunSource_RUN_SOURCE_SCHEDULE
	d.DueAt = timestamppb.New(time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC))
	f.dispatch(t, d)
	succeeded := f.nextFinish(t).GetSucceeded()
	require.NotNil(t, succeeded, "scheduled run did not succeed")

	assertJSON(t, succeeded.GetOutput(), `{"zero":true}`)
}
