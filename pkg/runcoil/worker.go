package runcoil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/google/uuid"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"

	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
)

const (
	protocolVersion = 1
	sdkName         = "runcoil-go"
	sdkVersion      = "0.1.0-dev"
	defaultSlots    = 10
	reconnectStart  = 500 * time.Millisecond
	reconnectCap    = 10 * time.Second
	registerTimeout = 10 * time.Second // before the server has answered Register
	minCallTimeout  = time.Second      // floor for unary calls after registration
	defaultLease    = 30 * time.Second
	ackTries        = 3
	retryStart      = 250 * time.Millisecond
	retryCap        = 5 * time.Second
	releaseBudget   = 5 * time.Second // total time a drain gives to finishing and releasing Runs
	handlerWait     = 2 * time.Second // part of the budget for cancelled handlers to return
)

// reminderEvery is how often an outage repeats its warning while it lasts.
var reminderEvery = time.Minute

// WorkerOption configures a Worker in NewWorker.
type WorkerOption func(*workerCfg)

// workerCfg holds the worker ID and what the options set, until NewWorker builds
// the Worker.
type workerCfg struct {
	workerID string
	slots    int
}

// validate checks the worker ID and the slots that NewWorker received. Its errors
// do not name the function; NewWorker adds that prefix.
func (cfg *workerCfg) validate() error {
	if cfg.workerID == "" {
		return errors.New("worker id is required")
	}

	if cfg.slots < 1 {
		return fmt.Errorf("slots must be at least 1, got %d", cfg.slots)
	}

	if uint64(cfg.slots) > math.MaxUint32 {
		return fmt.Errorf("slots %d is too large", cfg.slots)
	}

	return nil
}

// Worker runs the Tasks of one worker ID in this process. Add the Tasks with
// NewTask, call Start once, and call Shutdown to stop.
type Worker struct {
	client   *Client
	workerID string
	instance string // names this worker to the server; chosen once per Worker
	logger   *slog.Logger
	conn     *connection // the state of the connection to the server

	mu         sync.Mutex
	tasks      []*task
	byID       map[string]*task
	starting   bool
	started    bool
	stopped    bool
	hbInterval time.Duration
	lease      time.Duration
	attempts   map[string]*attempt // held attempts, by lease token

	slots     chan struct{}  // one token per running handler
	slotCount uint32         // the Slots value, which Register sends
	running   sync.WaitGroup // one count per attempt until its goroutine ends

	// hard bounds the work of a drain: the final reports and the releases. It is
	// cancelled when the release budget of a drain ends.
	hard       context.Context
	hardCancel context.CancelFunc

	listenStop context.CancelFunc
	listenDone chan struct{}
	beatStop   context.CancelFunc
	beatDone   chan struct{}

	shutdownOnce sync.Once
	shutdownErr  error
}

type task struct {
	id      string
	handler Handler
	def     *runcoilv1.TaskDefinition
}

// connection follows the state of the worker's connection to the server, not
// each attempt to reach it. The first failure of an outage is one warning; the
// failed attempts after it are debug lines; a reminder repeats the warning on a
// fixed interval; the first success after the outage is one info line. It is
// safe for concurrent use by the listen loop, the heartbeat loop, and the
// attempt goroutines.
type connection struct {
	logger   *slog.Logger
	workerID string
	instance string

	mu       sync.Mutex
	down     bool
	since    time.Time
	attempts int
	lastErr  error
	gen      uint64 // identifies the current outage; a timer of an older one does nothing

	// answered signals that the server has answered: the listen loop's wait ends early.
	answered chan struct{}
}

// NewWorker returns a Worker for the worker ID given by WorkerID, which must not
// be empty. The client must come from NewClient. The Worker does not contact the
// server until Start. Each Worker has its own instance ID, so several Workers can
// share a Client. A registration replaces the Tasks and schedules stored for the
// worker ID, so two different programs must not use the same worker ID. Options
// such as Slots change the defaults.
func NewWorker(client *Client, opts ...WorkerOption) (*Worker, error) {
	if client == nil || client.rpc == nil {
		return nil, errors.New("new worker: client must come from NewClient")
	}

	cfg := workerCfg{slots: defaultSlots}
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new worker: %w", err)
	}

	hard, hardCancel := context.WithCancel(context.Background())
	instance := uuid.NewString()
	return &Worker{
		client:    client,
		workerID:  cfg.workerID,
		instance:  instance,
		logger:    client.logger,
		slotCount: uint32(cfg.slots), //nolint:gosec // range checked above
		conn: &connection{
			logger:   client.logger,
			workerID: cfg.workerID,
			instance: instance,
			answered: make(chan struct{}, 1),
		},
		byID:       map[string]*task{},
		attempts:   map[string]*attempt{},
		slots:      make(chan struct{}, cfg.slots),
		hard:       hard,
		hardCancel: hardCancel,
	}, nil
}

// WorkerID names the worker. It is required. Every process that runs the same
// Tasks uses the same worker ID, and a Task is identified by the worker ID and
// its Task ID.
func WorkerID(id string) WorkerOption {
	return func(cfg *workerCfg) { cfg.workerID = id }
}

// Slots sets how many Runs the worker runs at one time. The server never
// gives the worker more. The default is 10. NewWorker rejects a value below 1
// or above 4294967295.
func Slots(n int) WorkerOption {
	return func(cfg *workerCfg) { cfg.slots = n }
}

// NewTask adds a Task with its handler to the worker. The id must be unique
// within the worker. It fails when the id is empty or taken, when the handler is
// zero, when an option is invalid, or when the worker has started. Call it
// before Start; Start registers the Task with the server.
func (w *Worker) NewTask(id string, h Handler, opts ...TaskOption) error {
	cfg := taskCfg{id: id, handler: h}
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return err
	}

	def := definition(cfg)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped {
		return fmt.Errorf("new task %q: worker is shut down", id)
	}

	if w.starting || w.started {
		return fmt.Errorf("new task %q: worker has already started", id)
	}

	if _, dup := w.byID[id]; dup {
		return fmt.Errorf("new task %q: task id is already registered", id)
	}

	t := &task{id: id, handler: h, def: def}
	w.tasks = append(w.tasks, t)
	w.byID[id] = t
	return nil
}

// Start registers the Tasks with the server and then listens for Runs in the
// background. It returns once registration succeeds, and returns the error when
// it fails. The context bounds only the registration call: the worker keeps
// running until Shutdown.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()

	switch {
	case w.stopped:
		w.mu.Unlock()
		return errors.New("start worker: worker is shut down")
	case w.starting || w.started:
		w.mu.Unlock()
		return errors.New("start worker: worker has already started")
	}

	w.starting = true
	w.mu.Unlock()

	if err := w.register(ctx, w.registerRequest()); err != nil {
		w.mu.Lock()
		w.starting = false
		w.mu.Unlock()
		return fmt.Errorf("start worker: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.starting = false
	if w.stopped {
		return errors.New("start worker: worker was shut down while starting")
	}

	listenCtx, listenStop := context.WithCancel(context.Background())
	beatCtx, beatStop := context.WithCancel(context.Background())

	w.started = true
	w.listenStop, w.listenDone = listenStop, make(chan struct{})
	w.beatStop, w.beatDone = beatStop, make(chan struct{})
	go w.listenLoop(listenCtx, w.listenDone)
	go w.heartbeatLoop(beatCtx, w.beatDone)
	return nil
}

// Shutdown stops the worker. It closes the Listen stream first, so the server
// stops dispatching, and then waits for the running handlers to finish. When
// drainCtx ends first, it cancels the handlers that are still running, waits up
// to 2 seconds for them to return, and reports each one to the server as
// released. The whole drain after drainCtx ends has a budget of 5 seconds; a
// result that is already being reported gets the same budget.
//
// A Step function must return promptly when its context is cancelled. Shutdown
// releases a Run only after its handler has returned or the 2 seconds have
// passed, so a function that keeps running after cancellation may still be
// running when another worker takes the Run.
//
// Shutdown returns an error only when a release could not be reported. Calling
// it again returns the first result.
func (w *Worker) Shutdown(drainCtx context.Context) error {
	w.shutdownOnce.Do(func() {
		w.shutdownErr = w.shutdown(drainCtx)
	})

	return w.shutdownErr
}

func (w *Worker) shutdown(drainCtx context.Context) error {
	w.mu.Lock()
	w.stopped = true
	listenStop, listenDone := w.listenStop, w.listenDone
	beatStop, beatDone := w.beatStop, w.beatDone
	w.mu.Unlock()

	if listenStop == nil {
		return nil // never started
	}

	listenStop()
	<-listenDone

	drained := make(chan struct{})
	go func() {
		w.running.Wait()
		close(drained)
	}()

	var err error
	select {
	case <-drained:
	case <-drainCtx.Done():
		err = w.releaseAll(drained)
	}

	// Heartbeats keep the leases of the attempts above until the last moment.
	beatStop()
	<-beatDone
	w.hardCancel()
	w.conn.stop()
	return err
}

// releaseAll ends a drain that has reached its deadline. It closes every attempt
// that has not reported, cancels its handler and its retry loops, and gives the
// handlers handlerWait to return, so that a Step function is not still running
// when the Run is handed to another worker. Then it sends FinishRun released for
// each of those attempts. Finally it waits, within the release budget, for the
// attempts whose final report was already in flight. The budget starts when
// releaseAll starts. A release the server has refused as stale is not an error.
func (w *Worker) releaseAll(drained <-chan struct{}) error {
	budget := time.AfterFunc(releaseBudget, w.hardCancel)
	defer budget.Stop()
	defer w.hardCancel()

	w.mu.Lock()
	held := make([]*attempt, 0, len(w.attempts))
	for _, a := range w.attempts {
		held = append(held, a)
	}

	w.mu.Unlock()

	// Claim first: a handler whose report then fails must not abandon the attempt.
	var closed []*attempt
	for _, a := range held {
		if !a.claim() {
			continue // already reporting, finished, or taken back by the server
		}

		a.cancel()
		a.lifeCancel()
		closed = append(closed, a)
	}

	window := make(chan struct{})
	timer := time.AfterFunc(handlerWait, func() { close(window) })
	defer timer.Stop()

	for _, a := range closed {
		select {
		case <-a.done:
		case <-window:
		}
	}

	err := w.releaseClosed(closed)

	select {
	case <-drained:
	case <-w.hard.Done():
	}

	return err
}

// releaseClosed sends FinishRun released for each attempt in closed, all at once,
// and waits for every send. It returns the failures; a stale lease is not one.
func (w *Worker) releaseClosed(closed []*attempt) error {
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)

	for _, a := range closed {
		wg.Add(1)
		go func(a *attempt) {
			defer wg.Done()
			if err := w.finish(w.hard, a, releasedResult()); err != nil && errorCode(err) != runcoilv1.ErrorCode_ERROR_CODE_STALE_LEASE {
				mu.Lock()
				errs = append(errs, fmt.Errorf("release run %s: %w", a.runID, err))
				mu.Unlock()
			}
		}(a)
	}

	wg.Wait()

	return errors.Join(errs...)
}

// registerRequest snapshots the Tasks into the message that Register sends.
func (w *Worker) registerRequest() *runcoilv1.RegisterRequest {
	w.mu.Lock()
	defer w.mu.Unlock()

	defs := make([]*runcoilv1.TaskDefinition, 0, len(w.tasks))
	for _, t := range w.tasks {
		defs = append(defs, t.def)
	}

	return &runcoilv1.RegisterRequest{
		WorkerId:              w.workerID,
		InstanceId:            w.instance,
		Tasks:                 defs,
		MaxConcurrentAttempts: w.slotCount,
		ProtocolVersion:       protocolVersion,
		SdkName:               sdkName,
		SdkVersion:            sdkVersion,
	}
}

// register sends the Tasks to the server and keeps its heartbeat and lease
// timings for the background loops.
func (w *Worker) register(ctx context.Context, req *runcoilv1.RegisterRequest) error {
	ctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()

	resp, err := w.client.rpc.Register(ctx, req)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	hb := resp.GetHeartbeatInterval().AsDuration()
	lease := resp.GetLeaseDuration().AsDuration()
	if hb <= 0 || lease <= 0 {
		return errors.New("register: server sent no heartbeat interval or lease duration")
	}

	w.mu.Lock()
	w.hbInterval = hb
	w.lease = lease
	w.mu.Unlock()
	return nil
}

// listenLoop keeps a Listen stream open until ctx ends. Each time the stream
// ends it waits, then opens a new one. The wait is the reconnect backoff: it
// grows while attempts fail, and it is reset as soon as the server answers
// anything, which also ends a wait early. A NOT_REGISTERED answer makes the
// worker register again, and a successful registration opens Listen at once.
func (w *Worker) listenLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	delay := reconnectStart
	immediate := false // the last listen answered NOT_REGISTERED and the worker registered again
	for {
		w.conn.drainAnswered()
		received, err := w.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}

		if received {
			delay = reconnectStart
		}

		wasImmediate := immediate
		immediate = false
		switch {
		case err == nil:
			w.logger.DebugContext(ctx, "listen stream ended by the server")
		case errorCode(err) == runcoilv1.ErrorCode_ERROR_CODE_NOT_REGISTERED:
			// The server answered: it forgot this worker. Register again; no outage.
			w.logger.DebugContext(ctx, "listen stream refused: not registered", slog.Any("error", err))
			if rerr := w.register(ctx, w.registerRequest()); rerr != nil {
				w.conn.failed(ctx, rerr, delay)
			} else {
				w.logger.InfoContext(ctx, "worker registered again", slog.String("instance_id", w.instance))
				delay = reconnectStart
				// Open Listen now, but only once in a row: a refusal straight after
				// a registration is a failure, and it waits as any failure does.
				immediate = !wasImmediate
			}
		default:
			w.conn.failed(ctx, err, delay)
		}

		if immediate {
			continue
		}

		running, answered := w.pause(ctx, delay)
		if !running {
			return
		}

		if answered {
			delay = reconnectStart
		} else {
			delay = min(2*delay, reconnectCap)
		}
	}
}

// pause waits for d before the next attempt. It returns early when the server
// answers. running is false when ctx ends first.
func (w *Worker) pause(ctx context.Context, d time.Duration) (running, answered bool) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false, false
	case <-timer.C:
		return true, false
	case <-w.conn.answered:
		return true, true
	}
}

// listenOnce opens one Listen stream and reads it until it ends. It reports
// whether any message arrived. The stream is dropped when no message, not even
// a keepalive, arrives for a lease duration.
func (w *Worker) listenOnce(ctx context.Context) (bool, error) {
	_, lease := w.timing()
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var stalled atomic.Bool
	watchdog := time.AfterFunc(lease, func() {
		stalled.Store(true)
		cancel()
	})

	defer watchdog.Stop()

	stream, err := w.client.rpc.Listen(streamCtx, &runcoilv1.ListenRequest{
		InstanceId: w.instance,
	})
	if err != nil {
		return false, err
	}

	defer func() { _ = stream.Close() }()

	received := false
	var streamErr error
	for {
		msg, err := stream.Receive()
		if err != nil {
			streamErr = err
			break
		}

		received = true
		watchdog.Reset(lease)
		w.conn.ok(ctx)
		w.handle(msg)
	}

	// io.EOF is the clean end of the stream, not a failure.
	if errors.Is(streamErr, io.EOF) {
		streamErr = nil
	}

	if stalled.Load() {
		return received, fmt.Errorf("no message within the lease of %s", lease)
	}

	return received, streamErr
}

// handle acts on one message from the Listen stream.
func (w *Worker) handle(msg *runcoilv1.ListenResponse) {
	if d := msg.GetDispatch(); d != nil {
		w.accept(d)
		return
	}

	if c := msg.GetCancel(); c != nil {
		w.abandon(c.GetRunId(), c.GetLeaseToken())
	}
	// A keepalive carries nothing; its arrival already reset the lease timer.
}

// accept takes a dispatched attempt and starts its goroutine. Attempts are
// keyed by lease token, so a dispatch that repeats a held attempt is ignored.
func (w *Worker) accept(d *runcoilv1.Dispatch) {
	w.mu.Lock()
	defer w.mu.Unlock()

	token := d.GetLeaseToken()
	if _, held := w.attempts[token]; held {
		w.logger.Warn("duplicate dispatch ignored", slog.String("run_id", d.GetRunId()))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	life, lifeCancel := context.WithCancel(context.Background())
	a := &attempt{
		runID:      d.GetRunId(),
		token:      token,
		cancel:     cancel,
		life:       life,
		lifeCancel: lifeCancel,
		done:       make(chan struct{}),
	}

	a.st = w.stateFor(d, a)
	a.ctx = runstate.With(ctx, a.st)
	w.attempts[token] = a

	// A nil task means the worker has no Task with that ID; execute reports it.
	t := w.byID[d.GetTaskId()]

	// Adding here is safe: Shutdown waits for the listen loop to end before it waits on running.
	w.running.Add(1)
	go w.execute(a, t, d)
}

// stateFor builds the run state of a dispatched attempt: its identity, the
// Steps recorded on earlier attempts, the tries already used, and the report
// function that sends each Step's outcome to the server.
func (w *Worker) stateFor(d *runcoilv1.Dispatch, a *attempt) *runstate.State {
	st := &runstate.State{
		RunID:      d.GetRunId(),
		WorkerID:   w.workerID,
		TaskID:     d.GetTaskId(),
		Attempt:    int(d.GetAttempt()),
		LeaseToken: d.GetLeaseToken(),
		Logger:     w.logger,
		Recorded:   make(map[string]runstate.Record, len(d.GetSteps())),
		Progress:   make(map[string]uint32, len(d.GetProgress())),
	}

	if d.GetSource() == runcoilv1.RunSource_RUN_SOURCE_SCHEDULE && d.GetDueAt() != nil {
		st.ScheduledAt = d.GetDueAt().AsTime()
	}

	for _, rec := range d.GetSteps() {
		st.Recorded[rec.GetStepId()] = recordOf(rec)
	}

	for _, p := range d.GetProgress() {
		st.Progress[p.GetStepId()] = p.GetAttemptsUsed()
	}

	st.Open = a.open
	st.Report = func(_ context.Context, r runstate.StepReport) (runstate.Record, bool) {
		return w.recordStep(a, r)
	}

	return st
}

// abandon stops a held attempt that the server has taken back.
func (w *Worker) abandon(runID, token string) {
	w.mu.Lock()
	a := w.attempts[token]
	w.mu.Unlock()

	if a == nil || a.runID != runID {
		return
	}

	w.logger.Info("attempt taken back by server", slog.String("run_id", runID))
	a.abandon()
}

// heartbeatLoop sends a heartbeat at the interval the server gave in Register,
// until ctx ends.
func (w *Worker) heartbeatLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	for {
		interval, _ := w.timing()
		if interval <= 0 {
			interval = registerTimeout
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		w.heartbeat(ctx)
	}
}

// heartbeat renews every held attempt and stops the ones the server reports as stale.
func (w *Worker) heartbeat(ctx context.Context) {
	w.mu.Lock()
	refs := make([]*runcoilv1.AttemptRef, 0, len(w.attempts))
	for _, a := range w.attempts {
		stepID, try, policy := a.runningStep()
		own, err := stepPolicyProto(policy)
		if err != nil {
			// step.Do validates the Step's policy before its function runs, so this does not happen; send none.
			w.logger.WarnContext(ctx, "running step policy not sent",
				slog.String("run_id", a.runID),
				slog.Any("error", err),
			)

			own = nil
		}

		refs = append(refs, &runcoilv1.AttemptRef{
			RunId:       a.runID,
			LeaseToken:  a.token,
			StepId:      stepID,
			StepAttempt: try,
			Retry:       own,
		})
	}

	w.mu.Unlock()

	callCtx, cancel := context.WithTimeout(ctx, w.callTimeout())
	defer cancel()

	resp, err := w.client.rpc.Heartbeat(callCtx, &runcoilv1.HeartbeatRequest{
		InstanceId: w.instance,
		Attempts:   refs,
	})
	if err != nil {
		if ctx.Err() == nil {
			next, _ := w.timing()
			w.conn.failed(ctx, err, next)
		}

		return
	}

	w.conn.ok(ctx)
	for _, s := range resp.GetStale() {
		w.abandon(s.GetRunId(), s.GetLeaseToken())
	}
}

// timing returns the heartbeat interval and the lease duration of the last registration.
func (w *Worker) timing() (heartbeat, lease time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hbInterval, w.lease
}

// leaseDuration returns the lease of the last registration, or the default before one.
func (w *Worker) leaseDuration() time.Duration {
	if _, lease := w.timing(); lease > 0 {
		return lease
	}

	return defaultLease
}

// callTimeout bounds one unary call: about one heartbeat interval, and the
// registration timeout until the server has answered Register.
func (w *Worker) callTimeout() time.Duration {
	hb, _ := w.timing()
	if hb <= 0 {
		return registerTimeout
	}

	return max(hb, minCallTimeout)
}

// retryCall runs op, and runs it again on a retryable failure: a transport
// failure, a server outage, or an internal server error. It stops after maxTries
// tries when maxTries is positive, once the next wait would pass budget, and as
// soon as parent ends. Each try has its own deadline. A failure that carries a
// Runcoil error code, such as STALE_LEASE, is never retried.
func (w *Worker) retryCall(parent context.Context, budget time.Duration, maxTries int, op func(ctx context.Context) error) error {
	deadline := time.Now().Add(budget)
	backoff := retryStart
	for try := 1; ; try++ {
		ctx, cancel := context.WithTimeout(parent, w.callTimeout())
		err := op(ctx)
		cancel()

		if err == nil || !retryable(err) || parent.Err() != nil {
			return err
		}

		if (maxTries > 0 && try >= maxTries) || time.Now().Add(backoff).After(deadline) {
			return err
		}

		w.logger.DebugContext(parent, "call failed; retrying",
			slog.Int("try", try),
			slog.Duration("next_delay", backoff),
			slog.Any("error", err),
		)

		if !sleep(parent, backoff) {
			return err
		}

		backoff = min(2*backoff, retryCap)
	}
}

// retryable reports whether a failed call may succeed when it is sent again.
func retryable(err error) bool {
	if errorCode(err) != runcoilv1.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return false
	}

	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeUnknown, connect.CodeInternal:
		return true
	default:
		return false
	}
}

// errorCode returns the Runcoil error code in the details of err, or
// ERROR_CODE_UNSPECIFIED when there is none.
func errorCode(err error) runcoilv1.ErrorCode {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return runcoilv1.ErrorCode_ERROR_CODE_UNSPECIFIED
	}

	for _, d := range ce.Details() {
		msg, err := connectproto.UnmarshalErrorDetail(d)
		if err != nil {
			continue
		}

		if detail, ok := msg.(*runcoilv1.ErrorDetail); ok {
			return detail.GetCode()
		}
	}

	return runcoilv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}

// sleep waits for d and reports false when ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// failed records a failed attempt to reach the server. next is the wait before
// the next attempt. The first failure of an outage is logged as a warning.
func (c *connection) failed(ctx context.Context, err error, next time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastErr = err
	if !c.down {
		c.down = true
		c.since = time.Now()
		c.attempts = 1
		c.gen++
		c.logger.WarnContext(ctx, "server connection lost",
			slog.Any("error", err),
			slog.String("worker_id", c.workerID),
			slog.String("instance_id", c.instance),
		)

		c.armReminder()
		return
	}

	c.attempts++
	c.logger.DebugContext(ctx, "server connection attempt failed",
		slog.Int("attempt", c.attempts),
		slog.Duration("next_delay", next),
		slog.Any("error", err),
	)
}

// ok records that the server answered: the listen stream delivered a message,
// or a heartbeat succeeded. It ends an outage with one info line.
func (c *connection) ok(ctx context.Context) {
	select {
	case c.answered <- struct{}{}:
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.down {
		return
	}

	c.logger.InfoContext(ctx, "server connection restored",
		slog.Duration("outage", time.Since(c.since)),
		slog.Int("attempts", c.attempts),
	)

	c.down = false
	c.attempts = 0
	c.lastErr = nil
	c.gen++
}

// drainAnswered discards an answer that the listen loop has not used yet, so
// that only answers from now on end its next wait.
func (c *connection) drainAnswered() {
	select {
	case <-c.answered:
	default:
	}
}

// stop ends the reminders when the worker shuts down.
func (c *connection) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.down = false
	c.gen++
}

// armReminder schedules the next reminder of the current outage. c.mu is held.
func (c *connection) armReminder() {
	gen := c.gen
	time.AfterFunc(reminderEvery, func() { c.remind(gen) })
}

// remind logs that the outage still lasts, and schedules the next reminder.
func (c *connection) remind(gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.down || gen != c.gen {
		return
	}

	c.logger.Warn("server still unreachable",
		slog.Duration("down_for", time.Since(c.since)),
		slog.Int("attempts", c.attempts),
		slog.Any("error", c.lastErr),
	)

	c.armReminder()
}
