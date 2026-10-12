package runcoil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"

	"connectrpc.com/connect"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
)

// errAttemptEnded is what callHandler returns when the server ended the attempt
// while the handler ran. Nothing is reported for such an attempt.
var errAttemptEnded = errors.New("attempt ended by the server")

// attempt is one Run attempt that this worker holds, from its dispatch until
// its result is reported or it is given back.
type attempt struct {
	runID string
	token string

	// ctx is the handler's context. It is cancelled when the server takes the
	// attempt back, or when a drain releases it.
	ctx    context.Context
	cancel context.CancelFunc

	// life ends the retry loops of the attempt: when the server takes it back,
	// or when the worker reaches the end of a drain.
	life       context.Context
	lifeCancel context.CancelFunc

	// done is closed when the attempt's goroutine has returned.
	done chan struct{}

	// st is the run state the handler sees. Heartbeats read the Step it names.
	st *runstate.State

	mu     sync.Mutex
	closed bool
}

// claim returns true for the first caller only. That caller sends the
// attempt's final report; every later caller must send nothing.
func (a *attempt) claim() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return false
	}

	a.closed = true
	return true
}

// open reports whether no caller has claimed the attempt yet.
func (a *attempt) open() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.closed
}

// runningStep returns the Step whose function runs in the attempt now, its try,
// and its own policy. The ID is "" when no Step function runs.
func (a *attempt) runningStep() (string, uint32, *runstate.RetryPolicy) {
	if a.st == nil {
		return "", 0, nil
	}

	return a.st.Running()
}

// abandon ends an attempt that the server has taken back. Its report would be
// refused, so none is sent, its handler's context is cancelled, and its retry
// loops end.
func (a *attempt) abandon() {
	a.claim()
	a.cancel()
	a.lifeCancel()
}

// panicError is a panic that the worker recovered from a handler.
type panicError struct {
	value any
	stack []byte
}

func (e *panicError) Error() string { return fmt.Sprintf("handler panicked: %v", e.value) }

// execute acknowledges a dispatch, runs its handler, and reports the result.
// It runs in its own goroutine, one per attempt.
func (w *Worker) execute(a *attempt, t *task, d *runcoilv1.Dispatch) {
	defer w.running.Done()
	defer close(a.done)
	defer w.forget(a)
	defer a.lifeCancel()
	defer a.cancel()

	if err := w.ack(a); err != nil {
		w.refuseDispatch(a, err)
		return
	}

	if t == nil {
		w.report(a, failedResult(&runcoilv1.AttemptError{
			Message:   fmt.Sprintf("task %q is not registered by this worker", d.GetTaskId()),
			Code:      runcoilv1.FailureCode_FAILURE_CODE_ERROR,
			Permanent: true,
		}, ""))

		return
	}

	if !w.takeSlot(a.ctx) {
		return // the attempt ended while it waited for a slot
	}

	defer func() { <-w.slots }()

	logger := w.logger.With(slog.String("run_id", a.runID))

	out, err := callHandler(a.ctx, t.handler, d.GetInput())
	if errors.Is(err, errAttemptEnded) {
		// The handler unwound. Normally the attempt was closed first, and nothing
		// is reported. If it is still open, the Run must not be stranded: release it.
		if a.claim() {
			logger.WarnContext(a.ctx, "handler unwound while the attempt was open; releasing the run")
			if ferr := w.finish(w.hard, a, releasedResult()); ferr != nil {
				w.logFinishError(a, ferr)
			}
		} else {
			logger.DebugContext(a.ctx, "attempt ended by the server; no report")
		}

		return
	}

	var pe *panicError
	if errors.As(err, &pe) {
		logger.ErrorContext(a.ctx, "handler panicked",
			slog.String("task_id", t.id),
			slog.Any("panic", pe.value),
			slog.String("stack", string(pe.stack)),
		)
	}

	w.report(a, resultOf(out, err))
}

// refuseDispatch ends an attempt whose dispatch the server did not acknowledge.
// A stale lease means the server took the attempt back, so the handler is not
// run and nothing is sent. Any other failure gives the Run back to the server.
func (w *Worker) refuseDispatch(a *attempt, err error) {
	logger := w.logger.With(slog.String("run_id", a.runID))
	if errorCode(err) == runcoilv1.ErrorCode_ERROR_CODE_STALE_LEASE {
		logger.InfoContext(a.ctx, "dispatch is stale; handler not run")
		a.abandon()
		return
	}

	logger.WarnContext(a.ctx, "dispatch not acknowledged; giving the run back", slog.Any("error", err))

	if a.claim() {
		if ferr := w.finish(w.hard, a, releasedResult()); ferr != nil {
			w.logFinishError(a, ferr)
		}
	}

	a.abandon()
}

// ack tells the server that this worker owns the attempt. A failure that the
// server does not refuse is retried a few times before the caller gives up.
func (w *Worker) ack(a *attempt) error {
	return w.retryCall(a.life, w.leaseDuration(), ackTries, func(ctx context.Context) error {
		_, err := w.client.rpc.AckDispatch(ctx, connect.NewRequest(&runcoilv1.AckDispatchRequest{
			RunId:      a.runID,
			LeaseToken: a.token,
		}))

		return err
	})
}

// takeSlot waits for one of the worker's handler slots. It returns false when
// ctx ends first.
func (w *Worker) takeSlot(ctx context.Context) bool {
	select {
	case w.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// report sends a result unless the attempt was already closed.
func (w *Worker) report(a *attempt, result *runcoilv1.FinishRunRequest) {
	if !a.claim() {
		w.logger.InfoContext(a.ctx, "attempt ended before its result was reported", slog.String("run_id", a.runID))
		return
	}

	// A claimed final report is not cut short by the drain: it runs on the
	// worker's release budget (see releaseAll).
	if err := w.finish(w.hard, a, result); err != nil {
		w.logFinishError(a, err)
	}
}

// logFinishError logs a FinishRun that failed for good. A refusal because the
// lease is stale is expected when the server took the attempt back.
func (w *Worker) logFinishError(a *attempt, err error) {
	logger := w.logger.With(slog.String("run_id", a.runID))
	if errorCode(err) == runcoilv1.ErrorCode_ERROR_CODE_STALE_LEASE {
		logger.InfoContext(a.ctx, "run result refused: lease is stale")
		return
	}

	logger.ErrorContext(a.ctx, "report run result failed", slog.Any("error", err))
}

// finish sends FinishRun, retrying transport failures for up to one lease and
// for as long as ctx lives. The call is idempotent by Run and attempt, so a
// retry cannot finish the attempt twice.
func (w *Worker) finish(ctx context.Context, a *attempt, result *runcoilv1.FinishRunRequest) error {
	result.RunId = a.runID
	result.LeaseToken = a.token

	var resp *connect.Response[runcoilv1.FinishRunResponse]
	err := w.retryCall(ctx, w.leaseDuration(), 0, func(ctx context.Context) error {
		var e error
		resp, e = w.client.rpc.FinishRun(ctx, connect.NewRequest(result))
		return e
	})
	if err != nil {
		return err
	}

	if retry := resp.Msg.GetRetry(); retry != nil {
		w.logger.DebugContext(a.ctx, "run will be attempted again",
			slog.String("run_id", a.runID),
			slog.Time("not_before", retry.GetNotBefore().AsTime()),
		)
	}

	return nil
}

// forget drops the attempt from the held set, so heartbeats stop naming it.
func (w *Worker) forget(a *attempt) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.attempts[a.token] == a {
		delete(w.attempts, a.token)
	}
}

// callHandler runs the handler and turns a panic into an error. The panic that
// ends an attempt becomes errAttemptEnded.
func callHandler(ctx context.Context, h Handler, input []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			if runstate.IsUnwind(r) {
				out, err = nil, errAttemptEnded
				return
			}

			out, err = nil, &panicError{value: r, stack: debug.Stack()}
		}
	}()

	return h.call(ctx, input)
}

// resultOf turns a handler's output or error into the FinishRun request.
func resultOf(out []byte, err error) *runcoilv1.FinishRunRequest {
	if err == nil {
		return &runcoilv1.FinishRunRequest{
			Result: &runcoilv1.FinishRunRequest_Succeeded{Succeeded: &runcoilv1.RunSucceeded{Output: out}},
		}
	}

	var se *runstate.StepError
	if errors.As(err, &se) {
		// The handler returned a Step's final error: the Run fails without a retry.
		ae := failureProto(failureOf(err))
		ae.Permanent = true
		return failedResult(ae, se.StepID)
	}

	return failedResult(failureProto(failureOf(err)), "")
}

// failureOf classifies a handler's error for the server.
func failureOf(err error) runstate.Failure {
	var pe *panicError
	if errors.As(err, &pe) {
		return runstate.Failure{Code: runstate.CodePanic, Message: pe.Error(), Permanent: true}
	}

	var ee *encodeError
	if errors.As(err, &ee) {
		return runstate.Failure{Code: runstate.CodeOutputNotEncodable, Message: ee.Error(), Permanent: true}
	}

	permanent, after := runstate.Classify(err)
	return runstate.Failure{Code: runstate.CodeError, Message: err.Error(), Permanent: permanent, RetryAfter: after}
}

// failureProto converts a failure to the message the server takes.
func failureProto(f runstate.Failure) *runcoilv1.AttemptError {
	ae := &runcoilv1.AttemptError{
		Message:   f.Message,
		Permanent: f.Permanent,
		Code:      failureCodeProto(f.Code),
	}

	if f.RetryAfter > 0 {
		ae.RetryAfter = durationpb.New(f.RetryAfter)
	}

	return ae
}

func failureCodeProto(code string) runcoilv1.FailureCode {
	switch code {
	case runstate.CodePanic:
		return runcoilv1.FailureCode_FAILURE_CODE_PANIC
	case runstate.CodeOutputNotEncodable:
		return runcoilv1.FailureCode_FAILURE_CODE_OUTPUT_NOT_ENCODABLE
	default:
		return runcoilv1.FailureCode_FAILURE_CODE_ERROR
	}
}

// failedResult builds a FinishRun that fails the Run. stepID is set when the
// handler returned the final error of that Step.
func failedResult(e *runcoilv1.AttemptError, stepID string) *runcoilv1.FinishRunRequest {
	return &runcoilv1.FinishRunRequest{
		Result: &runcoilv1.FinishRunRequest_Failed{Failed: &runcoilv1.HandlerFailed{Error: e, StepId: stepID}},
	}
}

func releasedResult() *runcoilv1.FinishRunRequest {
	return &runcoilv1.FinishRunRequest{
		Result: &runcoilv1.FinishRunRequest_Released{Released: &runcoilv1.Released{}},
	}
}

// recordStep sends one try of one Step of the attempt and returns the stored
// record. It returns false when the server ended the attempt, or when the
// report could not be made within a lease: the attempt is then ended here, and
// the handler must stop.
func (w *Worker) recordStep(a *attempt, r runstate.StepReport) (runstate.Record, bool) {
	logger := w.logger.With(slog.String("run_id", a.runID))

	req := &runcoilv1.RecordStepRequest{
		RunId:       a.runID,
		LeaseToken:  a.token,
		StepId:      r.StepID,
		StepAttempt: r.Attempt,
	}

	if r.Succeeded {
		req.Outcome = &runcoilv1.RecordStepRequest_Succeeded{
			Succeeded: &runcoilv1.StepSucceeded{Output: r.Output},
		}
	} else {
		req.Outcome = &runcoilv1.RecordStepRequest_Failed{Failed: failureProto(r.Failure)}
	}

	policy, err := stepPolicyProto(r.Policy)
	if err != nil {
		logger.ErrorContext(a.ctx, "step policy refused", slog.Any("error", err))

		a.abandon()
		return runstate.Record{}, false
	}

	req.Retry = policy

	var resp *connect.Response[runcoilv1.RecordStepResponse]
	err = w.retryCall(a.life, w.leaseDuration(), 0, func(ctx context.Context) error {
		var e error
		resp, e = w.client.rpc.RecordStep(ctx, connect.NewRequest(req))
		return e
	})
	if err != nil {
		w.logStepError(a, r.StepID, err)
		a.abandon()
		return runstate.Record{}, false
	}

	switch res := resp.Msg.GetResult().(type) {
	case *runcoilv1.RecordStepResponse_Recorded:
		return recordOf(res.Recorded), true
	case *runcoilv1.RecordStepResponse_Retry:
		logger.DebugContext(a.ctx, "step will be tried again; attempt over",
			slog.String("step_id", r.StepID),
			slog.Time("not_before", res.Retry.GetNotBefore().AsTime()),
		)
	}

	a.abandon()
	return runstate.Record{}, false
}

// logStepError logs a RecordStep that failed for good. A refusal because the
// lease is stale is expected when the server took the attempt back.
func (w *Worker) logStepError(a *attempt, stepID string, err error) {
	logger := w.logger.With(slog.String("run_id", a.runID))
	if errorCode(err) == runcoilv1.ErrorCode_ERROR_CODE_STALE_LEASE {
		logger.InfoContext(a.ctx, "step report refused: lease is stale")
		return
	}

	logger.ErrorContext(a.ctx, "step report failed; attempt ended",
		slog.String("step_id", stepID),
		slog.Any("error", err),
	)
}

// stepPolicyProto converts a Step's own policy to the message the server takes.
// It validates the policy first, as retryProto requires. A nil policy, meaning
// the Step has none, converts to nil.
func stepPolicyProto(p *runstate.RetryPolicy) (*runcoilv1.RetryPolicy, error) {
	if p == nil {
		return nil, nil
	}

	policy := RetryPolicy{MaxAttempts: p.MaxAttempts, Delays: p.Delays}
	if err := policy.validate(); err != nil {
		return nil, err
	}

	return retryProto(policy), nil
}

// recordOf converts a stored Step record from the server.
func recordOf(rec *runcoilv1.StepRecord) runstate.Record {
	if rec.GetStatus() == runcoilv1.StepStatus_STEP_STATUS_SUCCEEDED {
		return runstate.Record{Succeeded: true, Output: rec.GetOutput()}
	}

	return runstate.Record{
		Message:  rec.GetFailure().GetMessage(),
		Attempts: int(rec.GetFailure().GetAttempts()),
	}
}
