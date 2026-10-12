package runcoil_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
)

// syncBuffer is a log destination that the worker's goroutines may share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A drain that ends while a Step function runs must not turn the cancelled
// function into a failed try. The attempt is released, and nothing is recorded.
func TestDrainDuringStepUnwindsWithoutReport(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	inStep := make(chan struct{})
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		_, err := step.Do(ctx, "charge", func(ctx context.Context) (string, error) {
			close(inStep)
			<-ctx.Done()
			return "", ctx.Err()
		})

		return struct{}{}, err
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_d1", "tok_d1", "order", ""))
	select {
	case <-inStep:
	case <-time.After(5 * time.Second):
		require.Fail(t, "step did not start")
	}

	drain, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, w.Shutdown(drain), "shutdown")

	fin := f.nextFinish(t)
	require.Equal(t, "run_d1", fin.GetRunId())
	require.NotNil(t, fin.GetReleased(), "finish = run %q result %v, want released run_d1",
		fin.GetRunId(), fin.GetResult())

	require.Empty(t, f.stepRequests(), "Step reports sent for a cancelled attempt")

	f.expectNoFinish(t, 200*time.Millisecond)
}

// A stale lease between two Steps stops the handler before the second Step
// function runs, and nothing is reported for the attempt.
func TestStaleLeaseBetweenStepsSkipsNextStep(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	var secondRan atomic.Bool
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		if _, err := step.Do(ctx, "first", func(context.Context) (string, error) { return "one", nil }); err != nil {
			return struct{}{}, err
		}

		select {
		case <-ctx.Done(): // the server took the attempt back
		case <-time.After(5 * time.Second):
			return struct{}{}, errors.New("the attempt was never taken back")
		}

		_, err := step.Do(ctx, "second", func(context.Context) (string, error) {
			secondRan.Store(true)
			return "two", nil
		})

		return struct{}{}, err
	})))

	f.setRecordHook(func(req *runcoilv1.RecordStepRequest) (*runcoilv1.RecordStepResponse, error) {
		if req.GetStepId() == "first" {
			f.update(func(f *fakeServer) { f.heartbeatStale[req.GetRunId()] = true })
		}

		return nil, nil
	})

	h.start(t, w)

	f.dispatch(t, testDispatch("run_e1", "tok_e1", "order", ""))
	f.expectNoFinish(t, time.Second)
	require.False(t, secondRan.Load(), "second Step ran after the server took the attempt back")

	reqs := f.stepRequests()
	require.Len(t, reqs, 1, "step reports = %v, want only first", reqs)
	require.Equal(t, "first", reqs[0].GetStepId())
}

// A panic inside a Step is a failed try, and it is logged with its value and stack.
func TestPanicInStepIsLoggedAtErrorLevel(t *testing.T) {
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.workerFor(t, h.clientWith(t, runcoil.Logger(logger)), "demo")
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
			panic("gateway crashed")
		})

		return struct{}{}, err
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_x2", "tok_x2", "order", ""))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "finish did not fail")
	require.Equal(t, "charge", failed.GetStepId())

	out := logs.String()
	for _, want := range []string{"level=ERROR", "step panicked", "gateway crashed", "stack="} {
		require.Contains(t, out, want)
	}
}

// An internal server error on RecordStep is retried, and the Step is recorded.
func TestInternalErrorOnRecordStepIsRetried(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_1", nil })
		return struct{}{}, err
	})))

	var calls atomic.Int32
	f.setRecordHook(func(*runcoilv1.RecordStepRequest) (*runcoilv1.RecordStepResponse, error) {
		if calls.Add(1) == 1 {
			return nil, connect.NewError(connect.CodeInternal, errors.New("transient failure"))
		}

		return nil, nil
	})

	h.start(t, w)

	f.dispatch(t, testDispatch("run_i1", "tok_i1", "order", ""))
	require.NotNil(t, f.nextFinish(t).GetSucceeded(), "run did not succeed after the internal error")

	require.Len(t, f.stepRequests(), 2, "RecordStep calls")
}

// A handler stuck retrying a Step report ends with the drain, not a lease later.
func TestDrainEndsStuckStepReportPromptly(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_1", nil })
		return struct{}{}, err
	})))

	f.setRecordHook(func(*runcoilv1.RecordStepRequest) (*runcoilv1.RecordStepResponse, error) {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("server is down"))
	})

	h.start(t, w)

	f.dispatch(t, testDispatch("run_s9", "tok_s9", "order", ""))
	waitFor(t, "the first step report", func() bool { return len(f.stepRequests()) >= 1 })

	began := time.Now()
	drain, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, w.Shutdown(drain), "shutdown")

	elapsed := time.Since(began)
	require.LessOrEqual(t, elapsed, 1500*time.Millisecond, "the retry loop outlived the drain")

	fin := f.nextFinish(t)
	require.Equal(t, "run_s9", fin.GetRunId())
	require.NotNil(t, fin.GetReleased(), "finish = run %q result %v, want released run_s9",
		fin.GetRunId(), fin.GetResult())
}

// A timeout derived by the handler is an ordinary failure of the Step: it is
// reported, and the Run does not stay running on the server.
func TestHandlerDerivedTimeoutIsAFailedTry(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ struct{}) (struct{}, error) {
		ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		_, err := step.Do(ctx2, "slow", func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})

		return struct{}{}, err
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_td1", "tok_td1", "order", ""))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "finish did not fail")
	require.Equal(t, "slow", failed.GetStepId())

	req := recordedStep(t, f, "slow").GetFailed()
	require.NotNil(t, req, "no failed report for step slow")
	require.Contains(t, req.GetMessage(), "deadline exceeded", "step failure = %v, want the deadline error reported", req)
}

// A handler that unwinds while its attempt is still open must not strand the Run.
func TestUnwindWhileAttemptOpenReleasesRun(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(context.Context, struct{}) (struct{}, error) {
		runstate.Unwind()
		return struct{}{}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_uw1", "tok_uw1", "order", ""))
	fin := f.nextFinish(t)
	require.Equal(t, "run_uw1", fin.GetRunId())
	require.NotNil(t, fin.GetReleased(), "finish = run %q result %v, want released run_uw1",
		fin.GetRunId(), fin.GetResult())
}

// A cancel message from the server stops the handler without any report.
func TestCancelMessageStopsHandlerWithoutReport(t *testing.T) {
	f := newFakeServer()
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

	f.dispatch(t, testDispatch("run_c2", "tok_c2", "order", ""))
	<-started
	f.cancelAttempt(t, "run_c2", "tok_c2")
	<-returned
	f.expectNoFinish(t, 300*time.Millisecond)
}

type orderIn struct {
	OrderID string `json:"order_id"`
}

type orderOut struct {
	ChargeID string `json:"charge_id"`
}

// stepFixture is a worker with one Task, "order", whose handler is supplied by
// the test. It starts the worker and returns the fake that serves it.
func stepFixture(t *testing.T, handler func(ctx context.Context, in orderIn) (orderOut, error)) *fakeServer {
	t.Helper()
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(handler)))

	h.start(t, w)
	return f
}

// recordedStep returns the RecordStep request for a step ID, or fails the test.
func recordedStep(t *testing.T, f *fakeServer, stepID string) *runcoilv1.RecordStepRequest {
	t.Helper()
	for _, req := range f.stepRequests() {
		if req.GetStepId() == stepID {
			return req
		}
	}

	require.Failf(t, "no RecordStep found", "for %q; requests = %v", stepID, f.stepRequests())
	return nil
}

func TestStepSucceedsAndIsRecordedAsAttemptOne(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		charge, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_1", nil })
		if err != nil {
			return orderOut{}, err
		}

		return orderOut{ChargeID: charge}, nil
	})

	f.dispatch(t, testDispatch("run_s1", "tok_s1", "order", `{"order_id":"o_1"}`))
	succeeded := f.nextFinish(t).GetSucceeded()
	require.NotNil(t, succeeded, "run did not succeed")

	assertJSON(t, succeeded.GetOutput(), `{"charge_id":"ch_1"}`)

	req := recordedStep(t, f, "charge")
	require.EqualValues(t, 1, req.GetStepAttempt(), "record = %v", req)
	require.Equal(t, "run_s1", req.GetRunId(), "record = %v", req)
	require.Equal(t, "tok_s1", req.GetLeaseToken(), "record = %v", req)

	require.Equal(t, `"ch_1"`, string(req.GetSucceeded().GetOutput()))

	require.Nil(t, req.GetRetry(), "want the task's policy")
}

func TestReplayedSucceededStepIsDecodedAndNotCalled(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	var calls atomic.Int32
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ orderIn) (orderOut, error) {
		charge, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
			calls.Add(1)
			return "ch_new", nil
		})
		if err != nil {
			return orderOut{}, err
		}

		return orderOut{ChargeID: charge}, nil
	})))

	h.start(t, w)

	d := testDispatch("run_r2", "tok_r2", "order", `{"order_id":"o_2"}`)
	d.Attempt = 2
	d.Steps = []*runcoilv1.StepRecord{{
		StepId: "charge",
		Index:  1,
		Status: runcoilv1.StepStatus_STEP_STATUS_SUCCEEDED,
		Output: []byte(`"ch_1"`),
	}}

	f.dispatch(t, d)

	succeeded := f.nextFinish(t).GetSucceeded()
	require.NotNil(t, succeeded, "replayed run did not succeed")

	assertJSON(t, succeeded.GetOutput(), `{"charge_id":"ch_1"}`)
	require.Zero(t, calls.Load(), "step function ran on replay")

	require.Empty(t, f.stepRequests(), "replay sent RecordStep calls")
}

func TestReplayedFailedStepReturnsStepError(t *testing.T) {
	seen := make(chan step.Error, 1)
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	var calls atomic.Int32
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(ctx context.Context, _ orderIn) (orderOut, error) {
		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
			calls.Add(1)
			return "", errors.New("live error")
		})

		var se *step.Error
		if errors.As(err, &se) {
			seen <- *se
		}

		return orderOut{}, err
	})))

	h.start(t, w)

	d := testDispatch("run_r3", "tok_r3", "order", "")
	d.Steps = []*runcoilv1.StepRecord{{
		StepId: "charge",
		Index:  1,
		Status: runcoilv1.StepStatus_STEP_STATUS_FAILED,
		Failure: &runcoilv1.Failure{
			Code:     runcoilv1.FailureCode_FAILURE_CODE_ERROR,
			Message:  "card declined",
			Attempts: 3,
		},
	}}

	f.dispatch(t, d)

	se := <-seen
	require.Equal(t, "charge", se.StepID, "step error = %+v", se)
	require.Equal(t, 3, se.Attempts, "step error = %+v", se)
	require.Equal(t, "card declined", se.Message, "step error = %+v", se)

	require.Zero(t, calls.Load(), "step function ran on replay")

	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "finish = %v, want failed with step charge", failed)
	require.Equal(t, "charge", failed.GetStepId(), "finish = %v, want failed with step charge", failed)
	require.Equal(t, "card declined", failed.GetError().GetMessage(), "finish = %v, want failed with step charge", failed)
}

func TestRetryAnswerEndsAttemptWithoutFinish(t *testing.T) {
	reached := make(chan struct{}, 1)
	f := stepFixture(t, func(ctx context.Context, in orderIn) (orderOut, error) {
		charge, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_" + in.OrderID, nil })
		if err != nil {
			return orderOut{}, err
		}

		reached <- struct{}{}
		return orderOut{ChargeID: charge}, nil
	})

	f.setRecordHook(func(req *runcoilv1.RecordStepRequest) (*runcoilv1.RecordStepResponse, error) {
		if req.GetRunId() == "run_t1" {
			return retryAnswer(), nil
		}

		return nil, nil
	})

	f.dispatch(t, testDispatch("run_t1", "tok_t1", "order", `{"order_id":"o_t"}`))
	waitFor(t, "the step report", func() bool { return len(f.stepRequests()) == 1 })
	f.expectNoFinish(t, 200*time.Millisecond)
	select {
	case <-reached:
		require.Fail(t, "handler continued after a retry answer")
	default:
	}

	// The slot and the worker are free: the next dispatch runs to the end.
	f.dispatch(t, testDispatch("run_t2", "tok_t2", "order", `{"order_id":"o_t2"}`))
	fin := f.nextFinish(t)
	require.Equal(t, "run_t2", fin.GetRunId())
	require.NotNil(t, fin.GetSucceeded(), "second run finish = run %q result %v", fin.GetRunId(), fin.GetResult())
}

func TestProgressSetsStepAttempt(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		charge, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_1", nil })
		if err != nil {
			return orderOut{}, err
		}

		return orderOut{ChargeID: charge}, nil
	})

	d := testDispatch("run_p1", "tok_p1", "order", `{"order_id":"o_p"}`)
	d.Attempt = 2
	d.Progress = []*runcoilv1.StepProgress{{StepId: "charge", AttemptsUsed: 1}}
	f.dispatch(t, d)
	f.nextFinish(t)

	require.EqualValues(t, 2, recordedStep(t, f, "charge").GetStepAttempt())
}

func TestFinalStepFailureLetsHandlerCompensate(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		if _, err := step.Do(ctx, "reserve-stock", func(context.Context) (string, error) { return "res_1", nil }); err != nil {
			return orderOut{}, err
		}

		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
			return "", errors.New("card declined")
		})
		if err != nil {
			if _, rerr := step.Do(ctx, "release-stock", func(context.Context) (struct{}, error) {
				return struct{}{}, nil
			}); rerr != nil {
				return orderOut{}, rerr
			}

			return orderOut{}, err
		}

		return orderOut{}, nil
	})

	f.dispatch(t, testDispatch("run_c1", "tok_c1", "order", `{"order_id":"o_c"}`))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "run did not fail")

	require.Equal(t, "charge", failed.GetStepId())
	require.Contains(t, failed.GetError().GetMessage(), "card declined")

	require.NotNil(t, recordedStep(t, f, "charge").GetFailed(), "charge was not reported as a failure")

	require.NotNil(t, recordedStep(t, f, "release-stock").GetSucceeded(), "compensation did not run as a Step")
}

func TestStepFailureMarkersReachTheReport(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		_, _ = step.Do(ctx, "decline", func(context.Context) (string, error) {
			return "", runcoil.Permanent(errors.New("card declined"))
		})

		_, _ = step.Do(ctx, "limit", func(context.Context) (string, error) {
			return "", runcoil.RetryAfter(errors.New("rate limited"), 30*time.Second)
		})

		return orderOut{}, nil
	})

	f.dispatch(t, testDispatch("run_m1", "tok_m1", "order", ""))
	f.nextFinish(t)

	decline := recordedStep(t, f, "decline").GetFailed()
	require.True(t, decline.GetPermanent(), "decline failure = %v", decline)
	require.Nil(t, decline.GetRetryAfter())
	require.Equal(t, runcoilv1.FailureCode_FAILURE_CODE_ERROR, decline.GetCode())
	require.Equal(t, "card declined", decline.GetMessage())

	limit := recordedStep(t, f, "limit").GetFailed()
	require.False(t, limit.GetPermanent(), "limit failure = %v", limit)
	require.Equal(t, 30*time.Second, limit.GetRetryAfter().AsDuration())
	require.Equal(t, "rate limited", limit.GetMessage())
}

func TestRepeatedStepIDIsNumbered(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		var calls int
		_, err := step.Do(ctx, "", func(context.Context) (string, error) {
			calls++
			return "", nil
		})
		if err == nil || calls != 0 {
			return orderOut{}, errors.New("empty id must fail without running the function")
		}

		first, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_1", nil })
		if err != nil {
			return orderOut{}, err
		}

		second, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_2", nil })
		if err != nil {
			return orderOut{}, err
		}

		return orderOut{ChargeID: first + "+" + second}, nil
	})

	f.dispatch(t, testDispatch("run_n1", "tok_n1", "order", ""))
	succeeded := f.nextFinish(t).GetSucceeded()
	require.NotNil(t, succeeded, "run did not succeed")

	assertJSON(t, succeeded.GetOutput(), `{"charge_id":"ch_1+ch_2"}`)
	ids := map[string]bool{}
	for _, req := range f.stepRequests() {
		ids[req.GetStepId()] = true
	}

	require.Equal(t, map[string]bool{"charge": true, "charge#2": true}, ids, "step IDs")
}

func TestStepRetryPolicyIsSent(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) { return "ch_1", nil },
			step.Retry(step.RetryPolicy{MaxAttempts: 2, Delays: []time.Duration{time.Second}}))

		return orderOut{}, err
	})

	f.dispatch(t, testDispatch("run_k1", "tok_k1", "order", ""))
	f.nextFinish(t)
	policy := recordedStep(t, f, "charge").GetRetry()
	require.EqualValues(t, 2, policy.GetMaxAttempts())
	require.Len(t, policy.GetDelays(), 1, "retry = %v", policy)
	require.Equal(t, time.Second, policy.GetDelays()[0].AsDuration())
}

func TestPanicInsideStepIsReportedAsPanic(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		_, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
			panic("gateway crashed")
		})

		return orderOut{}, err
	})

	f.dispatch(t, testDispatch("run_x1", "tok_x1", "order", ""))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "finish did not fail")
	require.Equal(t, "charge", failed.GetStepId())

	req := recordedStep(t, f, "charge").GetFailed()
	require.Equal(t, runcoilv1.FailureCode_FAILURE_CODE_PANIC, req.GetCode())
	require.True(t, req.GetPermanent(), "panic failure = %v", req)
	require.Contains(t, req.GetMessage(), "gateway crashed", "panic failure = %v", req)
}

func TestOutputThatCannotBeEncodedIsReported(t *testing.T) {
	f := stepFixture(t, func(ctx context.Context, _ orderIn) (orderOut, error) {
		_, err := step.Do(ctx, "bad", func(context.Context) (chan int, error) {
			return make(chan int), nil
		})

		return orderOut{}, err
	})

	f.dispatch(t, testDispatch("run_e1", "tok_e1", "order", ""))
	f.nextFinish(t)
	req := recordedStep(t, f, "bad").GetFailed()
	require.Equal(t, runcoilv1.FailureCode_FAILURE_CODE_OUTPUT_NOT_ENCODABLE, req.GetCode())
	require.True(t, req.GetPermanent(), "encode failure = %v", req)
}

func TestHandlerErrorOutsideStepHasNoStepID(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("order", runcoil.HandlerFunc(func(_ context.Context, in orderIn) (orderOut, error) {
		if in.OrderID == "boom" {
			return orderOut{}, errors.New("boom")
		}

		return orderOut{ChargeID: "ch_ok"}, nil
	})))

	f.update(func(f *fakeServer) { f.finishRetry = true })
	h.start(t, w)

	f.dispatch(t, testDispatch("run_o1", "tok_o1", "order", `{"order_id":"boom"}`))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "run did not fail")
	require.Empty(t, failed.GetStepId(), "failed step ID, want none outside a Step")
	require.Equal(t, "boom", failed.GetError().GetMessage())
	require.False(t, failed.GetError().GetPermanent(), "failed = %v", failed)

	// The server answered retry to that FinishRun; the worker takes the next dispatch as usual.
	f.dispatch(t, testDispatch("run_o2", "tok_o2", "order", `{"order_id":"ok"}`))
	require.NotNil(t, f.nextFinish(t).GetSucceeded(), "worker did not run the next attempt")
}

func TestHandlerErrorIsReportedWithItsMarkers(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("modes", runcoil.HandlerFunc(func(_ context.Context, in modeIn) (struct{}, error) {
		switch in.Mode {
		case "permanent":
			return struct{}{}, runcoil.Permanent(errors.New("card declined"))
		case "retry":
			return struct{}{}, runcoil.RetryAfter(errors.New("rate limited"), 30*time.Second)
		}

		return struct{}{}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_2", "tok_2", "modes", `{"mode":"permanent"}`))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "permanent error did not fail the run")

	e := failed.GetError()
	require.Equal(t, "card declined", e.GetMessage())
	require.True(t, e.GetPermanent(), "attempt error = %v", e)
	require.Equal(t, runcoilv1.FailureCode_FAILURE_CODE_ERROR, e.GetCode())
	require.Nil(t, e.GetRetryAfter(), "attempt error = %v", e)

	require.Empty(t, failed.GetStepId(), "want none outside a Step")

	f.dispatch(t, testDispatch("run_3", "tok_3", "modes", `{"mode":"retry"}`))
	failed = f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "retry error did not fail the attempt")

	e = failed.GetError()
	require.Equal(t, "rate limited", e.GetMessage())
	require.False(t, e.GetPermanent(), "attempt error = %v", e)
	require.Equal(t, 30*time.Second, e.GetRetryAfter().AsDuration())
}

func TestHandlerPanicIsReportedAsPanic(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("modes", runcoil.HandlerFunc(func(_ context.Context, in modeIn) (struct{}, error) {
		if in.Mode == "panic" {
			panic("boom")
		}

		return struct{}{}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_4", "tok_4", "modes", `{"mode":"panic"}`))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "panic did not fail the run")

	e := failed.GetError()
	require.Equal(t, runcoilv1.FailureCode_FAILURE_CODE_PANIC, e.GetCode())
	require.Contains(t, e.GetMessage(), "boom", "attempt error = %v", e)

	// The worker survives the panic and runs the next attempt.
	f.dispatch(t, testDispatch("run_5", "tok_5", "modes", `{"mode":"ok"}`))
	require.NotNil(t, f.nextFinish(t).GetSucceeded(), "worker did not run the next attempt after a panic")
}

func TestStaleDispatchDoesNotRunHandler(t *testing.T) {
	f := newFakeServer()
	f.staleRuns["run_8"] = true
	h := newHarness(t, f)
	w := h.worker(t)
	var ran atomic.Bool
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (struct{}, error) {
		ran.Store(true)
		return struct{}{}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_8", "tok_8", "hello", ""))
	waitFor(t, "the ack", func() bool { return f.ackCount() == 1 })
	f.expectNoFinish(t, 200*time.Millisecond)
	require.False(t, ran.Load(), "handler ran after a stale acknowledgement")
}

func TestUnknownTaskFailsRun(t *testing.T) {
	f := newFakeServer()
	h := newHarness(t, f)
	w := h.worker(t)
	h.start(t, w)

	f.dispatch(t, testDispatch("run_9", "tok_9", "missing", ""))
	failed := f.nextFinish(t).GetFailed()
	require.NotNil(t, failed, "result for unknown task = %v", failed)
	require.Contains(t, failed.GetError().GetMessage(), `"missing"`)
	require.True(t, failed.GetError().GetPermanent(), "result for unknown task = %v", failed)
}

func TestAckRetriedThenRunGivenBackWhenItKeepsFailing(t *testing.T) {
	f := newFakeServer()
	f.update(func(f *fakeServer) { f.ackAlways = true })
	h := newHarness(t, f)
	w := h.worker(t)
	var ran atomic.Bool
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, _ struct{}) (struct{}, error) {
		ran.Store(true)
		return struct{}{}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_10", "tok_10", "hello", ""))
	fin := f.nextFinish(t)
	require.Equal(t, "run_10", fin.GetRunId())
	require.NotNil(t, fin.GetReleased(), "finish = run %q result %v, want released run_10", fin.GetRunId(), fin.GetResult())

	require.Equal(t, ackTriesForTest, f.ackCount())

	require.False(t, ran.Load(), "handler ran although the dispatch was never acknowledged")
}

// ackTriesForTest is the number of AckDispatch calls the worker makes before it gives up.
const ackTriesForTest = 3

func TestFinishRetriedAcrossShortOutage(t *testing.T) {
	f := newFakeServer()
	f.update(func(f *fakeServer) { f.finishOutage = 2 })
	h := newHarness(t, f)
	w := h.worker(t)
	require.NoError(t, w.NewTask("hello", runcoil.HandlerFunc(func(_ context.Context, in helloIn) (helloOut, error) {
		return helloOut{Greeting: "hello, " + in.Name}, nil
	})))

	h.start(t, w)

	f.dispatch(t, testDispatch("run_11", "tok_11", "hello", `{"name":"linus"}`))
	succeeded := f.nextFinish(t).GetSucceeded()
	require.NotNil(t, succeeded, "result was lost during the outage")

	assertJSON(t, succeeded.GetOutput(), `{"greeting":"hello, linus"}`)
}
