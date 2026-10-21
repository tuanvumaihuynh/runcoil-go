// Package step runs the Steps of a handler. A Step is a function whose outcome
// Runcoil records, so that a retried Run replays the Steps that succeeded and
// does not run them again.
package step

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"

	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
)

// RetryPolicy is runcoil.RetryPolicy, so a handler file needs one import for a Step.
type RetryPolicy = runcoil.RetryPolicy

// Error is the final error of a Step that failed for good: its tries are used
// up, or its error was permanent. Do returns it for such a Step, also on replay,
// and errors.As finds it. It is rebuilt from the stored text, so errors.Is does
// not match the application's own error values.
type Error = runstate.StepError

// cfg holds the Step's id and what the options set, until Do uses it.
type cfg struct {
	id    string
	retry RetryPolicy
}

// validate checks the Step's id and its own policy. Its errors name the Step.
func (cfg *cfg) validate() error {
	if cfg.id == "" {
		return errors.New("step: id is required")
	}

	if cfg.retry.MaxAttempts < 0 {
		return fmt.Errorf("step %q: retry max attempts must not be negative", cfg.id)
	}

	if int64(cfg.retry.MaxAttempts) > math.MaxUint32 {
		return fmt.Errorf("step %q: retry max attempts %d is too large", cfg.id, cfg.retry.MaxAttempts)
	}

	for _, d := range cfg.retry.Delays {
		if d <= 0 {
			return fmt.Errorf("step %q: retry delays must be greater than 0", cfg.id)
		}
	}

	return nil
}

// Option changes one Step call. Build one with Retry.
type Option func(*cfg)

// Retry overrides the retry policy of the Task for this Step. A zero field
// means the server's default.
func Retry(p RetryPolicy) Option {
	return func(cfg *cfg) { cfg.retry = p }
}

// Do runs fn and returns its result. Wrap each call with a side effect, such as
// a payment or an email, in Do. The first success is stored, so a retried Run
// returns the stored value and does not call fn again. A failure that is not
// yet final ends the attempt: the Run is tried again later, and the handler
// does not continue. A failure that is final is returned as *Error, and the
// handler may run a fallback or a compensation.
//
// Outside a Run, Do runs fn and records nothing, and returns its result and
// error untouched.
//
// A handler that calls Do must follow two rules:
//
//   - Call Do on the handler's own goroutine. The SDK ends an attempt by
//     panicking inside Do, and only the handler's goroutine recovers that panic.
//   - Code that recovers panics must re-panic any value it does not recognise.
//     The SDK stops a handler this way when the server schedules a retry, and
//     a recover that swallows the value would let the handler run on.
//
// Once the server takes the attempt back, or the worker gives it back at the
// end of a drain, Do stops the handler and reports nothing. A Step function
// that returns an error because a context derived by the handler expired, such
// as a timeout the handler set, is an ordinary failed try and is reported. The
// Step's function gets the context it was given, and a Step function must return
// promptly when that context is cancelled. While a Step's function runs, the
// heartbeats of the attempt name that Step.
func Do[T any](ctx context.Context, id string, fn func(ctx context.Context) (T, error), opts ...Option) (T, error) {
	st, inRun := runstate.From(ctx)
	if !inRun {
		return fn(ctx)
	}

	var zero T
	cfg := cfg{id: id}
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return zero, err
	}

	if st.Report == nil || st.Open == nil {
		return zero, fmt.Errorf("step %q: run state is incomplete", id)
	}

	if !st.Open() {
		runstate.Unwind()
	}

	stepID := beginStep(ctx, st, id)
	if rec, ok := st.Lookup(stepID); ok {
		return result[T](stepID, rec)
	}

	attemptNo := st.Tries(stepID) + 1
	policy := policyOf(cfg.retry)
	out := runStep(ctx, st, stepID, attemptNo, policy, fn)
	if !st.Open() {
		// The worker took the attempt back while fn ran. Whatever fn returned is not reported.
		runstate.Unwind()
	}

	if out.panicked {
		logPanic(ctx, st, stepID, out.panicValue, out.stack)
	}

	rep := stepReport(stepID, attemptNo, policy, out)
	rec, ok := st.Report(ctx, rep)
	if !ok {
		runstate.Unwind()
	}

	st.Store(stepID, rec)
	return result[T](stepID, rec)
}

// beginStep counts this use of id in the Run and returns the ID the Step is stored
// under. A repeated use is logged, because its Step is then stored as a numbered one.
func beginStep(ctx context.Context, st *runstate.State, id string) string {
	stepID, repeated := st.Begin(id)
	if repeated {
		st.Logger.WarnContext(ctx, "step id used again in this run; stored as a numbered step",
			slog.String("step_id", stepID),
			slog.String("run_id", st.RunID),
		)
	}

	return stepID
}

// logPanic logs a Step function that panicked. The panic is not raised again:
// Do reports it as a failed try.
func logPanic(ctx context.Context, st *runstate.State, stepID string, panicValue any, stack []byte) {
	st.Logger.ErrorContext(ctx, "step panicked",
		slog.String("run_id", st.RunID),
		slog.String("step_id", stepID),
		slog.Any("panic", panicValue),
		slog.String("stack", string(stack)),
	)
}

// stepReport builds the report of one try from what its function returned or
// the panic it raised.
func stepReport[T any](stepID string, attempt uint32, policy *runstate.RetryPolicy, out outcome[T]) runstate.StepReport {
	rep := runstate.StepReport{
		StepID:  stepID,
		Attempt: attempt,
		Policy:  policy,
	}

	switch {
	case out.panicked:
		rep.Failure = runstate.Failure{
			Code:      runstate.CodePanic,
			Message:   fmt.Sprintf("step panicked: %v", out.panicValue),
			Permanent: true,
		}
	case out.err != nil:
		permanent, after := runstate.Classify(out.err)
		rep.Failure = runstate.Failure{
			Code:       runstate.CodeError,
			Message:    out.err.Error(),
			Permanent:  permanent,
			RetryAfter: after,
		}
	default:
		b, err := json.Marshal(out.value)
		if err != nil {
			rep.Failure = runstate.Failure{
				Code:      runstate.CodeOutputNotEncodable,
				Message:   fmt.Sprintf("encode output: %v", err),
				Permanent: true,
			}
		} else {
			rep.Succeeded = true
			rep.Output = b
		}
	}

	return rep
}

// runStep runs fn with its Step recorded as running, so that heartbeats name it.
// The record is cleared when fn returns, also when fn panics.
func runStep[T any](ctx context.Context, st *runstate.State, stepID string, attempt uint32, policy *runstate.RetryPolicy, fn func(ctx context.Context) (T, error)) outcome[T] {
	st.SetRunning(stepID, attempt, policy)
	defer st.ClearRunning()

	return call(ctx, fn)
}

// outcome is what fn returned, or the panic it raised.
type outcome[T any] struct {
	value      T
	err        error
	panicked   bool
	panicValue any
	stack      []byte
}

// call runs fn and recovers its panic. The panic that ends an attempt is not
// fn's failure, so it is raised again.
func call[T any](ctx context.Context, fn func(ctx context.Context) (T, error)) (out outcome[T]) {
	defer func() {
		if r := recover(); r != nil {
			if runstate.IsUnwind(r) {
				panic(r)
			}

			out = outcome[T]{panicked: true, panicValue: r, stack: debug.Stack()}
		}
	}()

	out.value, out.err = fn(ctx)
	return out
}

// result turns a stored record into what Do returns: the decoded value, or the
// *Error of a failed Step.
func result[T any](stepID string, rec runstate.Record) (T, error) {
	var zero T
	if !rec.Succeeded {
		return zero, &Error{StepID: stepID, Attempts: rec.Attempts, Message: rec.Message}
	}

	var v T
	if err := json.Unmarshal(rec.Output, &v); err != nil {
		return zero, fmt.Errorf("step %q: decode stored output: %w", stepID, err)
	}

	return v, nil
}

// policyOf converts the Step's policy. A policy with no field set is sent as
// nil, so the Task's policy applies.
func policyOf(p RetryPolicy) *runstate.RetryPolicy {
	if p.MaxAttempts == 0 && len(p.Delays) == 0 {
		return nil
	}

	return &runstate.RetryPolicy{MaxAttempts: p.MaxAttempts, Delays: p.Delays}
}
