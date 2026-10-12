package runcoil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
	"github.com/tuanvumaihuynh/runcoil-go/internal/schedspec"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/schedule"
)

// TaskOption configures one Task in Worker.NewTask.
type TaskOption func(*taskCfg)

// taskCfg holds the id, the handler, and what the options set, until NewTask
// builds the Task definition.
type taskCfg struct {
	id            string
	handler       Handler
	retry         RetryPolicy
	sched         schedule.Schedule
	hasSchedule   bool
	startDeadline time.Duration
}

// validate checks the Task that NewTask received. Its errors name the Task, as
// NewTask's errors do.
func (cfg *taskCfg) validate() error {
	if cfg.id == "" {
		return errors.New("new task: id is required")
	}

	if cfg.handler.call == nil {
		return fmt.Errorf("new task %q: handler is required", cfg.id)
	}

	if cfg.hasSchedule {
		if err := schedspec.Validate(cfg.sched); err != nil {
			return fmt.Errorf("new task %q: %w", cfg.id, err)
		}
	}

	if err := cfg.retry.validate(); err != nil {
		return fmt.Errorf("new task %q: %w", cfg.id, err)
	}

	if cfg.startDeadline < 0 {
		return fmt.Errorf("new task %q: start deadline must not be negative", cfg.id)
	}

	return nil
}

// Handler is the code of one Task. Build it with HandlerFunc.
type Handler struct {
	call func(ctx context.Context, input []byte) ([]byte, error)
}

// RetryPolicy is shared by a Task and a Step. A zero field means the default.
type RetryPolicy struct {
	// MaxAttempts is the number of attempts in total, the first one included.
	// Zero means the server's default.
	MaxAttempts int
	// Delays is the wait before each retry. The last value repeats. Nil means
	// the server's default.
	Delays []time.Duration
}

// RunInfo describes the Run and the attempt that a handler is working on.
type RunInfo struct {
	RunID    string
	WorkerID string // the worker ID the Task is registered under
	TaskID   string
	// Attempt counts from 1.
	Attempt int
	// ScheduledAt is the tick that created the Run. It is zero unless a schedule created the Run.
	ScheduledAt time.Time
}

// encodeError marks an output that could not be encoded to JSON.
type encodeError struct {
	err error
}

func (e *encodeError) Error() string { return "encode output: " + e.err.Error() }
func (e *encodeError) Unwrap() error { return e.err }

// Schedule starts a Run of the Task on the given schedule. A Task without this
// option has no schedule. A later registration of the Task without it removes
// the schedule.
func Schedule(s schedule.Schedule) TaskOption {
	return func(cfg *taskCfg) {
		cfg.sched = s
		cfg.hasSchedule = true
	}
}

// Retry sets the retry policy of every Step of the Task. Zero fields mean the
// server's defaults.
func Retry(p RetryPolicy) TaskOption {
	return func(cfg *taskCfg) { cfg.retry = p }
}

// StartDeadline fails a Run that has not started d after it was due, for
// example a scheduled Run that no worker picked up in time. Zero means no
// deadline: a late Run still runs. A negative d is rejected by NewTask.
func StartDeadline(d time.Duration) TaskOption {
	return func(cfg *taskCfg) { cfg.startDeadline = d }
}

// HandlerFunc adapts a typed function to a Handler. The input is decoded from
// JSON; an empty input decodes to the zero value of In. The output is encoded
// to JSON when fn returns no error. Use it for every Task of a worker.
func HandlerFunc[In, Out any](fn func(context.Context, In) (Out, error)) Handler {
	if fn == nil {
		return Handler{}
	}

	return Handler{call: func(ctx context.Context, raw []byte) ([]byte, error) {
		var in In
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, Permanent(fmt.Errorf("decode input: %w", err))
			}
		}

		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}

		b, err := json.Marshal(out)
		if err != nil {
			return nil, &encodeError{err: err}
		}

		return b, nil
	}}
}

// Permanent marks err as one that must not be retried. A Step whose function
// returns it fails at once, and a handler that returns it fails the attempt
// without a retry. It returns nil when err is nil, and keeps err reachable with
// errors.Is and errors.As.
func Permanent(err error) error {
	return runstate.Permanent(err)
}

// RetryAfter marks err as retryable after d, which replaces the retry policy's
// delay for the next try. A failure marked this way still counts as an attempt.
// It returns nil when err is nil, and keeps err reachable with errors.Is and errors.As.
func RetryAfter(err error, d time.Duration) error {
	return runstate.RetryAfter(err, d)
}

// Info returns the Run that ctx belongs to. Call it inside a handler. Outside
// a handler it returns the zero RunInfo.
func Info(ctx context.Context) RunInfo {
	s, ok := runstate.From(ctx)
	if !ok {
		return RunInfo{}
	}

	return RunInfo{
		RunID:       s.RunID,
		WorkerID:    s.WorkerID,
		TaskID:      s.TaskID,
		Attempt:     s.Attempt,
		ScheduledAt: s.ScheduledAt,
	}
}

// definition converts a validated Task to the definition that Register sends.
// NewTask validates the Task before it calls this.
func definition(cfg taskCfg) *runcoilv1.TaskDefinition {
	def := &runcoilv1.TaskDefinition{TaskId: cfg.id}

	if cfg.hasSchedule {
		def.Schedule = schedspec.Proto(cfg.sched)
	}

	def.Retry = retryProto(cfg.retry)

	if cfg.startDeadline > 0 {
		def.StartDeadline = durationpb.New(cfg.startDeadline)
	}

	return def
}

// validate checks the policy: a negative value, or a max attempts that does not
// fit in the message. Its error text starts with "retry:".
func (p RetryPolicy) validate() error {
	if p.MaxAttempts < 0 {
		return errors.New("retry: max attempts must not be negative")
	}

	if uint64(p.MaxAttempts) > math.MaxUint32 {
		return fmt.Errorf("retry: max attempts %d is too large", p.MaxAttempts)
	}

	for _, d := range p.Delays {
		if d < 0 {
			return errors.New("retry: delays must not be negative")
		}
	}

	return nil
}

// retryProto converts a policy that the caller has validated. A policy with no
// field set is sent as nil, so the server applies its default.
func retryProto(p RetryPolicy) *runcoilv1.RetryPolicy {
	if p.MaxAttempts == 0 && len(p.Delays) == 0 {
		return nil
	}

	out := &runcoilv1.RetryPolicy{MaxAttempts: uint32(p.MaxAttempts)} //nolint:gosec // the caller validated the policy
	for _, d := range p.Delays {
		out.Delays = append(out.Delays, durationpb.New(d))
	}

	return out
}
