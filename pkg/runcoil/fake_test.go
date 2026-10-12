package runcoil_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1/runcoilv1connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testToken = "dev-token"

// recordHook answers one RecordStep call. A nil error with a nil response
// means the default answer.
type recordHook func(req *runcoilv1.RecordStepRequest) (*runcoilv1.RecordStepResponse, error)

// fakeServer implements the worker side of the server in memory. A test pushes
// dispatches into the open Listen stream of an instance and reads what the
// worker sent back.
type fakeServer struct {
	runcoilv1connect.UnimplementedWorkerServiceHandler

	hbInterval time.Duration
	lease      time.Duration

	finishes chan *runcoilv1.FinishRunRequest
	drop     chan struct{} // each value ends the open Listen stream, as a server would

	mu             sync.Mutex
	badAuth        int
	registers      []*runcoilv1.RegisterRequest
	registerErr    error
	listens        int
	notRegistered  int                                       // Listen answers NOT_REGISTERED this many more times
	pushes         map[string]chan *runcoilv1.ListenResponse // by instance ID
	lastInstance   string
	staleRuns      map[string]bool // AckDispatch answers STALE_LEASE for these runs
	ackUnavailable int             // AckDispatch answers Unavailable this many more times
	ackAlways      bool            // AckDispatch always answers Unavailable
	acks           []string        // every AckDispatch call, by run ID
	finishOutage   int             // FinishRun answers Unavailable this many more times
	finishRetry    bool            // the next FinishRun answers retry
	heartbeats     [][]*runcoilv1.AttemptRef
	heartbeatStale map[string]bool // Heartbeat names these runs as stale
	heartbeatDown  bool            // Heartbeat answers unavailable while set
	registerTimes  []time.Time     // when each Register arrived
	listenTimes    []time.Time     // when each Listen arrived
	steps          []*runcoilv1.RecordStepRequest
	recordHook     recordHook
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		hbInterval:     50 * time.Millisecond,
		lease:          2 * time.Second,
		finishes:       make(chan *runcoilv1.FinishRunRequest, 64),
		drop:           make(chan struct{}, 4),
		pushes:         map[string]chan *runcoilv1.ListenResponse{},
		staleRuns:      map[string]bool{},
		heartbeatStale: map[string]bool{},
	}
}

func (f *fakeServer) checkAuth(h http.Header) {
	if h.Get("Authorization") != "Bearer "+testToken {
		f.mu.Lock()
		f.badAuth++
		f.mu.Unlock()
	}
}

// pushLocked returns the dispatch channel of an instance. f.mu must be held.
func (f *fakeServer) pushLocked(instance string) chan *runcoilv1.ListenResponse {
	ch, ok := f.pushes[instance]
	if !ok {
		ch = make(chan *runcoilv1.ListenResponse)
		f.pushes[instance] = ch
	}

	return ch
}

func (f *fakeServer) Register(_ context.Context, req *connect.Request[runcoilv1.RegisterRequest]) (*connect.Response[runcoilv1.RegisterResponse], error) {
	f.checkAuth(req.Header())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers = append(f.registers, req.Msg)
	f.registerTimes = append(f.registerTimes, time.Now())
	f.lastInstance = req.Msg.GetInstanceId()
	f.pushLocked(f.lastInstance)
	if f.registerErr != nil {
		return nil, f.registerErr
	}

	return connect.NewResponse(&runcoilv1.RegisterResponse{
		HeartbeatInterval: durationpb.New(f.hbInterval),
		LeaseDuration:     durationpb.New(f.lease),
	}), nil
}

func (f *fakeServer) Listen(ctx context.Context, req *connect.Request[runcoilv1.ListenRequest], stream *connect.ServerStream[runcoilv1.ListenResponse]) error {
	f.checkAuth(req.Header())
	f.mu.Lock()
	f.listens++
	f.listenTimes = append(f.listenTimes, time.Now())
	reject := f.notRegistered > 0
	if reject {
		f.notRegistered--
	}

	push := f.pushLocked(req.Msg.GetInstanceId())
	f.mu.Unlock()
	if reject {
		err := connect.NewError(connect.CodeFailedPrecondition, errors.New("instance is not registered"))
		detail, derr := connect.NewErrorDetail(&runcoilv1.ErrorDetail{Code: runcoilv1.ErrorCode_ERROR_CODE_NOT_REGISTERED})
		if derr != nil {
			return derr
		}

		err.AddDetail(detail)
		return err
	}

	ticker := time.NewTicker(f.hbInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			keepalive := &runcoilv1.ListenResponse{Message: &runcoilv1.ListenResponse_Keepalive{Keepalive: &runcoilv1.Keepalive{}}}
			if err := stream.Send(keepalive); err != nil {
				return err
			}
		case msg := <-push:
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-f.drop:
			return nil
		}
	}
}

func (f *fakeServer) Heartbeat(_ context.Context, req *connect.Request[runcoilv1.HeartbeatRequest]) (*connect.Response[runcoilv1.HeartbeatResponse], error) {
	f.checkAuth(req.Header())
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heartbeatDown {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("heartbeat is down"))
	}

	f.heartbeats = append(f.heartbeats, req.Msg.GetAttempts())
	var stale []*runcoilv1.AttemptRef
	for _, a := range req.Msg.GetAttempts() {
		if f.heartbeatStale[a.GetRunId()] {
			stale = append(stale, a)
		}
	}

	return connect.NewResponse(&runcoilv1.HeartbeatResponse{Stale: stale}), nil
}

func (f *fakeServer) AckDispatch(_ context.Context, req *connect.Request[runcoilv1.AckDispatchRequest]) (*connect.Response[runcoilv1.AckDispatchResponse], error) {
	f.checkAuth(req.Header())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, req.Msg.GetRunId())
	if f.ackAlways || f.ackUnavailable > 0 {
		if !f.ackAlways {
			f.ackUnavailable--
		}

		return nil, connect.NewError(connect.CodeUnavailable, errors.New("server is down"))
	}

	if f.staleRuns[req.Msg.GetRunId()] {
		err := connect.NewError(connect.CodeFailedPrecondition, errors.New("lease is stale"))
		detail, derr := connect.NewErrorDetail(&runcoilv1.ErrorDetail{Code: runcoilv1.ErrorCode_ERROR_CODE_STALE_LEASE})
		if derr != nil {
			return nil, derr
		}

		err.AddDetail(detail)
		return nil, err
	}

	return connect.NewResponse(&runcoilv1.AckDispatchResponse{}), nil
}

func (f *fakeServer) RecordStep(_ context.Context, req *connect.Request[runcoilv1.RecordStepRequest]) (*connect.Response[runcoilv1.RecordStepResponse], error) {
	f.checkAuth(req.Header())
	f.mu.Lock()
	f.steps = append(f.steps, req.Msg)
	hook := f.recordHook
	f.mu.Unlock()

	if hook != nil {
		resp, err := hook(req.Msg)
		if err != nil {
			return nil, err
		}

		if resp != nil {
			return connect.NewResponse(resp), nil
		}
	}

	return connect.NewResponse(defaultRecordAnswer(req.Msg)), nil
}

// defaultRecordAnswer stores a success as SUCCEEDED and a failure as FAILED, the
// way the server does when a try is final.
func defaultRecordAnswer(req *runcoilv1.RecordStepRequest) *runcoilv1.RecordStepResponse {
	rec := &runcoilv1.StepRecord{StepId: req.GetStepId(), Index: 1}
	if s := req.GetSucceeded(); s != nil {
		rec.Status = runcoilv1.StepStatus_STEP_STATUS_SUCCEEDED
		rec.Output = s.GetOutput()
	} else {
		rec.Status = runcoilv1.StepStatus_STEP_STATUS_FAILED
		rec.Failure = &runcoilv1.Failure{
			Code:     req.GetFailed().GetCode(),
			Message:  req.GetFailed().GetMessage(),
			Attempts: req.GetStepAttempt(),
		}
	}

	return &runcoilv1.RecordStepResponse{Result: &runcoilv1.RecordStepResponse_Recorded{Recorded: rec}}
}

// retryAnswer is the server's answer when a Step will be tried again.
func retryAnswer() *runcoilv1.RecordStepResponse {
	return &runcoilv1.RecordStepResponse{Result: &runcoilv1.RecordStepResponse_Retry{Retry: &runcoilv1.RetryScheduled{
		NotBefore:    timestamppb.Now(),
		AttemptsUsed: 1,
	}}}
}

func (f *fakeServer) FinishRun(_ context.Context, req *connect.Request[runcoilv1.FinishRunRequest]) (*connect.Response[runcoilv1.FinishRunResponse], error) {
	f.checkAuth(req.Header())
	f.mu.Lock()
	if f.finishOutage > 0 {
		f.finishOutage--
		f.mu.Unlock()
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("server is down"))
	}

	retry := f.finishRetry
	f.finishRetry = false
	f.mu.Unlock()

	f.finishes <- req.Msg
	if retry {
		return connect.NewResponse(&runcoilv1.FinishRunResponse{Result: &runcoilv1.FinishRunResponse_Retry{
			Retry: &runcoilv1.RetryScheduled{NotBefore: timestamppb.Now(), AttemptsUsed: 1},
		}}), nil
	}

	return connect.NewResponse(&runcoilv1.FinishRunResponse{}), nil
}

// update changes the fake's state under its lock, before or during a test.
func (f *fakeServer) update(fn func(*fakeServer)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// registerRequests returns the Register calls received so far.
func (f *fakeServer) registerRequests() []*runcoilv1.RegisterRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*runcoilv1.RegisterRequest(nil), f.registers...)
}

// setRecordHook installs the answer for RecordStep calls.
func (f *fakeServer) setRecordHook(h recordHook) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHook = h
}

// instanceOf returns the instance ID that registered the worker ID.
func (f *fakeServer) instanceOf(t *testing.T, workerID string) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.registers {
		if r.GetWorkerId() == workerID {
			return r.GetInstanceId()
		}
	}

	require.Failf(t, "no registration", "for worker ID %q", workerID)
	return ""
}

// dispatch sends one dispatch to the last instance that registered.
func (f *fakeServer) dispatch(t *testing.T, d *runcoilv1.Dispatch) {
	t.Helper()
	f.mu.Lock()
	instance := f.lastInstance
	f.mu.Unlock()
	f.dispatchTo(t, instance, d)
}

// dispatchTo sends one dispatch down the open Listen stream of an instance.
func (f *fakeServer) dispatchTo(t *testing.T, instance string, d *runcoilv1.Dispatch) {
	t.Helper()
	f.pushTo(t, instance, &runcoilv1.ListenResponse{Message: &runcoilv1.ListenResponse_Dispatch{Dispatch: d}})
}

// cancelAttempt sends a cancel message for an attempt to the last instance that registered.
func (f *fakeServer) cancelAttempt(t *testing.T, runID, token string) {
	t.Helper()
	f.mu.Lock()
	instance := f.lastInstance
	f.mu.Unlock()
	f.pushTo(t, instance, &runcoilv1.ListenResponse{Message: &runcoilv1.ListenResponse_Cancel{
		Cancel: &runcoilv1.CancelAttempt{RunId: runID, LeaseToken: token},
	}})
}

// pushTo sends one message down the open Listen stream of an instance.
func (f *fakeServer) pushTo(t *testing.T, instance string, msg *runcoilv1.ListenResponse) {
	t.Helper()
	f.mu.Lock()
	push := f.pushLocked(instance)
	f.mu.Unlock()
	select {
	case push <- msg:
	case <-time.After(5 * time.Second):
		require.Fail(t, "no Listen stream took the message")
	}
}

// dropStream ends the open Listen stream of the server, as a server does when it
// drops a stream.
func (f *fakeServer) dropStream() {
	f.drop <- struct{}{}
}

// heartbeatRefsOf returns the attempt reference of each heartbeat that names the run.
func (f *fakeServer) heartbeatRefsOf(runID string) []*runcoilv1.AttemptRef {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*runcoilv1.AttemptRef
	for _, refs := range f.heartbeats {
		for _, r := range refs {
			if r.GetRunId() == runID {
				out = append(out, r)
			}
		}
	}

	return out
}

// heartbeatSteps returns, for each heartbeat that names the run, the stored Step ID
// it reported. An empty ID means no Step function ran.
func (f *fakeServer) heartbeatSteps(runID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, refs := range f.heartbeats {
		for _, r := range refs {
			if r.GetRunId() == runID {
				out = append(out, r.GetStepId())
			}
		}
	}

	return out
}

// nextFinish returns the next FinishRun call, or fails the test.
func (f *fakeServer) nextFinish(t *testing.T) *runcoilv1.FinishRunRequest {
	t.Helper()
	select {
	case fin := <-f.finishes:
		return fin
	case <-time.After(5 * time.Second):
		require.Fail(t, "no FinishRun arrived")
		return nil
	}
}

// expectNoFinish fails the test when a FinishRun arrives within d.
func (f *fakeServer) expectNoFinish(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case fin := <-f.finishes:
		require.Failf(t, "unexpected FinishRun", "for %s: %v", fin.GetRunId(), fin.GetResult())
	case <-time.After(d):
	}
}

// stepRequests returns the RecordStep calls received so far.
func (f *fakeServer) stepRequests() []*runcoilv1.RecordStepRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*runcoilv1.RecordStepRequest(nil), f.steps...)
}

func (f *fakeServer) registerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.registers)
}

func (f *fakeServer) listenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listens
}

func (f *fakeServer) ackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acks)
}

func (f *fakeServer) badAuthCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.badAuth
}

// heartbeatNamed reports whether a heartbeat has named the run's attempt.
func (f *fakeServer) heartbeatNamed(runID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, refs := range f.heartbeats {
		for _, r := range refs {
			if r.GetRunId() == runID {
				return true
			}
		}
	}

	return false
}

// harness serves a fakeServer over HTTP/1.1 and builds clients for it.
type harness struct {
	fake   *fakeServer
	server *httptest.Server
	down   atomic.Bool // when set, every connection is closed before any answer
}

// setDown makes the server refuse connections (true) or accept them again (false).
func (h *harness) setDown(down bool) {
	h.down.Store(down)
}

func newHarness(t *testing.T, f *fakeServer) *harness {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := runcoilv1connect.NewWorkerServiceHandler(f)
	mux.Handle(path, handler)
	h := &harness{fake: f}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.down.Load() {
			// Close the connection without an answer: the client sees an unavailable server.
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
					return
				}
			}

			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		mux.ServeHTTP(w, r)
	}))

	// Cleanups run last in, first out: the server closes after the workers shut down.
	t.Cleanup(srv.Close)
	h.server = srv
	return h
}

func (h *harness) client(t *testing.T) *runcoil.Client {
	t.Helper()
	return h.clientWith(t)
}

// clientWith returns a client for the fake, with extra options such as a logger.
func (h *harness) clientWith(t *testing.T, extra ...runcoil.ClientOption) *runcoil.Client {
	t.Helper()
	opts := append([]runcoil.ClientOption{runcoil.Endpoint(h.server.URL), runcoil.Token(testToken)}, extra...)
	c, err := runcoil.NewClient(opts...)
	require.NoError(t, err)

	return c
}

// worker returns a worker with worker ID "demo" that is shut down at the end of the test.
func (h *harness) worker(t *testing.T) *runcoil.Worker {
	t.Helper()
	return h.workerFor(t, h.client(t), "demo")
}

// workerFor returns a worker with the given worker ID on a client, with the given
// options, shut down at the end of the test. The worker ID comes before opts, so a
// WorkerID in opts replaces it.
func (h *harness) workerFor(t *testing.T, c *runcoil.Client, workerID string, opts ...runcoil.WorkerOption) *runcoil.Worker {
	t.Helper()

	opts = append([]runcoil.WorkerOption{runcoil.WorkerID(workerID)}, opts...)
	w, err := runcoil.NewWorker(c, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		drain, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Shutdown(drain)
	})

	return w
}

// start starts the worker and fails the test on error.
func (h *harness) start(t *testing.T, w *runcoil.Worker) {
	t.Helper()
	require.NoError(t, w.Start(context.Background()))
}

func testDispatch(runID, token, taskID string, input string) *runcoilv1.Dispatch {
	d := &runcoilv1.Dispatch{
		RunId:      runID,
		Attempt:    1,
		LeaseToken: token,
		TaskId:     taskID,
		Source:     runcoilv1.RunSource_RUN_SOURCE_INVOKE,
	}
	if input != "" {
		d.Input = []byte(input)
	}

	return d
}

// assertJSON fails the test unless got and want are the same JSON value.
func assertJSON(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	require.NoError(t, json.Unmarshal(got, &g), "output %q is not JSON", got)

	require.NoError(t, json.Unmarshal([]byte(want), &w))

	require.Equal(t, w, g, "output = %s, want %s", got, want)
}

// waitFor polls cond until it holds, or fails the test.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 5*time.Millisecond, "timed out waiting for %s", what)
}

// listenAfter returns when the first Listen after t arrived, and whether one has.
func (f *fakeServer) listenAfter(t time.Time) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, at := range f.listenTimes {
		if at.After(t) {
			return at, true
		}
	}

	return time.Time{}, false
}

// lastRegisterTime returns when the latest Register arrived.
func (f *fakeServer) lastRegisterTime() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.registerTimes) == 0 {
		return time.Time{}
	}

	return f.registerTimes[len(f.registerTimes)-1]
}
