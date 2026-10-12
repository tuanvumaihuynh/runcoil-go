// Package runstate holds the value a worker puts in a handler's context, and
// the types that the public packages share without importing each other. It is
// internal: runcoil reads and fills it, and step reads and reports through it.
package runstate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Failure codes of a Step try, as the server names them.
const (
	CodeError              = "ERROR"
	CodePanic              = "PANIC"
	CodeOutputNotEncodable = "OUTPUT_NOT_ENCODABLE"
)

// State describes one attempt of one Run. The worker builds it from the
// dispatch it received and puts it in the context it passes to the handler.
type State struct {
	RunID       string
	WorkerID    string
	TaskID      string
	Attempt     int
	ScheduledAt time.Time // zero unless a schedule created the Run
	LeaseToken  string
	Logger      *slog.Logger

	// Recorded holds the outcomes of the Steps that finished on earlier
	// attempts, by stored Step ID. Read it with Lookup.
	Recorded map[string]Record
	// Progress holds the tries already used by Steps that have no record, by
	// stored Step ID. It never changes after the worker builds the State.
	Progress map[string]uint32

	// Open reports whether the attempt still belongs to this worker. It turns
	// false once the server has taken the attempt back, or once the worker has
	// given it back at the end of a drain.
	Open func() bool

	// Report sends one try of one Step to the server and waits for its answer.
	// It returns the stored record and true when the Step is finished. It returns
	// false when the server ended the attempt, or the attempt could not report:
	// the worker has then ended the attempt, and the caller must Unwind.
	Report func(ctx context.Context, r StepReport) (Record, bool)

	mu             sync.Mutex
	seen           map[string]int
	running        string // stored ID of the Step whose function runs now, or ""
	runningAttempt uint32
	runningPolicy  *RetryPolicy // the running Step's own policy, nil when it has none
}

// Record is the stored outcome of one Step of one Run: an output, or a failure.
type Record struct {
	Succeeded bool
	Output    []byte // JSON, when Succeeded
	Message   string // the last error's text, when not Succeeded
	Attempts  int    // tries used, when not Succeeded
}

// StepReport is one try of one Step, as the SDK sends it.
type StepReport struct {
	StepID    string // the stored ID, numbered for a repeat as in "charge#2"
	Attempt   uint32 // try of this Step, from 1
	Succeeded bool
	Output    []byte  // JSON, when Succeeded
	Failure   Failure // when not Succeeded
	Policy    *RetryPolicy
}

// Failure describes one failed try: its code, text, and retry markers.
type Failure struct {
	Code       string
	Message    string
	Permanent  bool
	RetryAfter time.Duration // zero when the error gives no delay
}

// RetryPolicy is a retry policy as the SDK sends it. A zero field means the default.
type RetryPolicy struct {
	MaxAttempts int
	Delays      []time.Duration
}

// Begin counts one use of id in this execution. It returns the ID that the
// Step is stored under: id itself on the first use, and id#n from the second use
// on. It reports whether the use is a repeat.
func (s *State) Begin(id string) (stepID string, repeated bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[string]int{}
	}

	s.seen[id]++
	n := s.seen[id]
	if n == 1 {
		return id, false
	}

	return fmt.Sprintf("%s#%d", id, n), true
}

// Lookup returns the record of a Step stored by an earlier attempt or by this one.
func (s *State) Lookup(stepID string) (Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.Recorded[stepID]
	return r, ok
}

// Store keeps the record of a Step that finished in this attempt.
func (s *State) Store(stepID string, r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Recorded == nil {
		s.Recorded = map[string]Record{}
	}

	s.Recorded[stepID] = r
}

// SetRunning records that the function of a Step, try number attempt, is about
// to run, with the Step's own policy (nil when it has none). Steps run one at a
// time on the handler's goroutine.
func (s *State) SetRunning(stepID string, attempt uint32, policy *RetryPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = stepID
	s.runningAttempt = attempt
	s.runningPolicy = policy
}

// ClearRunning records that no Step function runs now.
func (s *State) ClearRunning() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = ""
	s.runningAttempt = 0
	s.runningPolicy = nil
}

// Running returns the stored ID, the try number, and the own policy of the Step
// whose function runs now. The ID is empty when no Step function runs.
// Heartbeats read it.
func (s *State) Running() (stepID string, attempt uint32, policy *RetryPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running, s.runningAttempt, s.runningPolicy
}

// Tries returns the tries already used by a Step with no record.
func (s *State) Tries(stepID string) uint32 {
	return s.Progress[stepID]
}

// StepError is the final error of a Step that failed for good. It is rebuilt
// from the stored text, so it never matches the application's own error values.
type StepError struct {
	StepID   string
	Attempts int
	Message  string
}

// Error returns the recorded message of the Step's last error.
func (e *StepError) Error() string { return e.Message }

// unwind is the panic value that stops a handler whose attempt the server has ended.
type unwind struct{}

// Unwind stops the handler of an attempt that the server has ended. Only the
// SDK panics with it. Code that recovers panics must re-panic it, as IsUnwind
// reports.
func Unwind() {
	panic(unwind{})
}

// IsUnwind reports whether a recovered panic value is the one Unwind raises.
func IsUnwind(v any) bool {
	_, ok := v.(unwind)
	return ok
}

type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as one that must not be retried. It returns nil when err is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return &permanentError{err: err}
}

type retryAfterError struct {
	err   error
	after time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter marks err as retryable after d. It returns nil when err is nil.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		return nil
	}

	return &retryAfterError{err: err, after: d}
}

// Classify reports the markers in err's chain. An error that holds both
// markers is permanent.
func Classify(err error) (permanent bool, retryAfter time.Duration) {
	var p *permanentError
	permanent = errors.As(err, &p)
	var ra *retryAfterError
	if errors.As(err, &ra) {
		retryAfter = ra.after
	}

	return permanent, retryAfter
}

type key struct{}

// With returns a copy of ctx that carries s.
func With(ctx context.Context, s *State) context.Context {
	return context.WithValue(ctx, key{}, s)
}

// From returns the state carried by ctx, and false when ctx did not come from
// a worker's dispatch.
func From(ctx context.Context) (*State, bool) {
	s, ok := ctx.Value(key{}).(*State)
	return s, ok && s != nil
}
