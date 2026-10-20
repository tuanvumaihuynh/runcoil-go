package runcoil_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1/runcoilv1connect"
)

func TestNewClientValidatesOptions(t *testing.T) {
	cases := []struct {
		name string
		opts []runcoil.ClientOption
		ok   bool
	}{
		{"valid https", []runcoil.ClientOption{runcoil.Endpoint("https://runcoil.internal:8080"), runcoil.Token("t")}, true},
		{"valid http", []runcoil.ClientOption{runcoil.Endpoint("http://localhost:7070"), runcoil.Token("t")}, true},
		{"no endpoint", []runcoil.ClientOption{runcoil.Token("t")}, false},
		{"no token", []runcoil.ClientOption{runcoil.Endpoint("http://localhost:7070")}, false},
		{"grpc scheme", []runcoil.ClientOption{runcoil.Endpoint("grpc://localhost:7070"), runcoil.Token("t")}, false},
		{"no host", []runcoil.ClientOption{runcoil.Endpoint("http://"), runcoil.Token("t")}, false},
		{"not a url", []runcoil.ClientOption{runcoil.Endpoint("://bad"), runcoil.Token("t")}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runcoil.NewClient(tc.opts...)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// fakeRuns implements the RunService in memory. Each method records its request
// and answers with its hook, which receives the number of the call to that
// method, from 1. A method with no hook answers unimplemented.
type fakeRuns struct {
	runcoilv1connect.UnimplementedRunServiceHandler

	invoke   func(n int, req *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error)
	getRun   func(n int, req *runcoilv1.GetRunRequest) (*runcoilv1.GetRunResponse, error)
	waitRun  func(n int, req *runcoilv1.WaitRunRequest) (*runcoilv1.WaitRunResponse, error)
	listRuns func(n int, req *runcoilv1.ListRunsRequest) (*runcoilv1.ListRunsResponse, error)
	rerun    func(n int, req *runcoilv1.RerunRequest) (*runcoilv1.RerunResponse, error)

	mu    sync.Mutex
	calls []any // every request received, in order
}

// answer records req, checks the bearer token, and returns the hook's answer.
func answer[Req, Resp any](ctx context.Context, f *fakeRuns, hook func(int, Req) (Resp, error), req Req) (Resp, error) {
	var zero Resp
	info, _ := connect.CallInfoForServerContext(ctx)
	if info.RequestHeader().Get("Authorization") != "Bearer "+testToken {
		return zero, connect.NewError(connect.CodeUnauthenticated, "bad token")
	}

	f.mu.Lock()
	f.calls = append(f.calls, req)
	f.mu.Unlock()

	if hook == nil {
		return zero, connect.NewError(connect.CodeUnimplemented, "no hook")
	}

	return hook(len(received[Req](f)), req)
}

// received returns the requests of type Req that the fake has received, in order.
func received[Req any](f *fakeRuns) []Req {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []Req
	for _, c := range f.calls {
		if r, ok := c.(Req); ok {
			out = append(out, r)
		}
	}

	return out
}

func (f *fakeRuns) Invoke(ctx context.Context, req *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
	return answer(ctx, f, f.invoke, req)
}

func (f *fakeRuns) GetRun(ctx context.Context, req *runcoilv1.GetRunRequest) (*runcoilv1.GetRunResponse, error) {
	return answer(ctx, f, f.getRun, req)
}

func (f *fakeRuns) WaitRun(ctx context.Context, req *runcoilv1.WaitRunRequest) (*runcoilv1.WaitRunResponse, error) {
	return answer(ctx, f, f.waitRun, req)
}

func (f *fakeRuns) ListRuns(ctx context.Context, req *runcoilv1.ListRunsRequest) (*runcoilv1.ListRunsResponse, error) {
	return answer(ctx, f, f.listRuns, req)
}

func (f *fakeRuns) Rerun(ctx context.Context, req *runcoilv1.RerunRequest) (*runcoilv1.RerunResponse, error) {
	return answer(ctx, f, f.rerun, req)
}

// newRunsClient serves f over httptest and returns a client for it, with the
// logger given. A nil logger discards the SDK's lines.
func newRunsClient(t *testing.T, f *fakeRuns, logger *slog.Logger) *runcoil.Client {
	t.Helper()
	server := connect.NewServer()
	runcoilv1connect.RegisterRunServiceHandler(server, f)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, server, connecthttp.WithReadMaxBytes(0))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	opts := []runcoil.ClientOption{runcoil.Endpoint(srv.URL), runcoil.Token(testToken)}
	if logger != nil {
		opts = append(opts, runcoil.Logger(logger))
	}

	c, err := runcoil.NewClient(opts...)
	require.NoError(t, err)
	return c
}

// runcoilErr returns a connect error that carries the Runcoil ErrorDetail.
func runcoilErr(t *testing.T, code connect.Code, msg string, detail *runcoilv1.ErrorDetail) error {
	t.Helper()
	d, err := connectproto.NewErrorDetail(detail)
	require.NoError(t, err)
	return connect.NewError(code, msg).WithDetail(d)
}

func TestInvokeRetriesWithOneGeneratedKey(t *testing.T) {
	f := &fakeRuns{invoke: func(n int, _ *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		if n <= 2 {
			return nil, connect.NewError(connect.CodeUnavailable, "server is shutting down")
		}

		return &runcoilv1.InvokeResponse{RunId: "run-1"}, nil
	}}
	logs := &capture{}
	c := newRunsClient(t, f, slog.New(logs))

	inv, err := c.Invoke(context.Background(), runcoil.InvokeRequest{WorkerID: "accounts", TaskID: "create-account"})
	require.NoError(t, err)

	reqs := received[*runcoilv1.InvokeRequest](f)
	require.Len(t, reqs, 3)
	key := reqs[0].GetIdempotencyKey()
	for _, r := range reqs {
		require.Equal(t, key, r.GetIdempotencyKey(), "every try sends the same key")
	}

	id, err := uuid.Parse(key)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), id.Version())
	assert.Equal(t, key, inv.IdempotencyKey)
	assert.Equal(t, "run-1", inv.RunID)
	assert.Equal(t, 2, logs.count(slog.LevelDebug, "call failed; retrying"))
}

func TestInvokeKeepsGivenKey(t *testing.T) {
	f := &fakeRuns{invoke: func(int, *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		return &runcoilv1.InvokeResponse{RunId: "run-1", Deduplicated: true}, nil
	}}
	c := newRunsClient(t, f, nil)

	inv, err := c.Invoke(context.Background(), runcoil.InvokeRequest{
		WorkerID:       "accounts",
		TaskID:         "create-account",
		IdempotencyKey: "onboarding/acct-ada",
	})
	require.NoError(t, err)

	reqs := received[*runcoilv1.InvokeRequest](f)
	require.Len(t, reqs, 1)
	assert.Equal(t, "onboarding/acct-ada", reqs[0].GetIdempotencyKey())
	assert.Equal(t, runcoil.Invocation{RunID: "run-1", Deduplicated: true, IdempotencyKey: "onboarding/acct-ada"}, inv)
}

func TestRetryStopsWhenContextEnds(t *testing.T) {
	f := &fakeRuns{invoke: func(int, *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		return nil, connect.NewError(connect.CodeUnavailable, "server is shutting down")
	}}
	c := newRunsClient(t, f, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	_, err := c.Invoke(ctx, runcoil.InvokeRequest{
		WorkerID:       "accounts",
		TaskID:         "create-account",
		IdempotencyKey: "k",
	})
	require.Error(t, err)

	var rcErr *runcoil.Error
	require.ErrorAs(t, err, &rcErr)
	assert.Empty(t, rcErr.Code)
	assert.Equal(t, "k", rcErr.IdempotencyKey)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	var connectErr *connect.Error
	assert.ErrorAs(t, err, &connectErr)
	assert.Equal(t, connect.CodeUnavailable, connectErr.Code())
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Contains(t, err.Error(), "unavailable")
}

func TestIdempotencyConflictMapsCodeAndRunIDWithoutRetry(t *testing.T) {
	f := &fakeRuns{invoke: func(int, *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		return nil, runcoilErr(t, connect.CodeAlreadyExists, "key is taken", &runcoilv1.ErrorDetail{
			Code:  runcoilv1.ErrorCode_ERROR_CODE_IDEMPOTENCY_CONFLICT,
			RunId: "run-1",
		})
	}}
	c := newRunsClient(t, f, nil)

	_, err := c.Invoke(context.Background(), runcoil.InvokeRequest{
		WorkerID:       "accounts",
		TaskID:         "create-account",
		IdempotencyKey: "k",
	})

	var rcErr *runcoil.Error
	require.ErrorAs(t, err, &rcErr)
	assert.Equal(t, runcoil.CodeIdempotencyConflict, rcErr.Code)
	assert.Equal(t, "run-1", rcErr.RunID)
	assert.Equal(t, "k", rcErr.IdempotencyKey)
	assert.Equal(t, "idempotency_conflict: key is taken", rcErr.Error())
	assert.Len(t, received[*runcoilv1.InvokeRequest](f), 1)
}

func TestTaskNotFoundIsNotRetried(t *testing.T) {
	f := &fakeRuns{invoke: func(int, *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		return nil, runcoilErr(t, connect.CodeNotFound, "no such task", &runcoilv1.ErrorDetail{
			Code: runcoilv1.ErrorCode_ERROR_CODE_TASK_NOT_FOUND,
		})
	}}
	c := newRunsClient(t, f, nil)

	_, err := c.Invoke(context.Background(), runcoil.InvokeRequest{
		WorkerID:       "accounts",
		TaskID:         "missing",
		IdempotencyKey: "k",
	})

	var rcErr *runcoil.Error
	require.ErrorAs(t, err, &rcErr)
	assert.Equal(t, runcoil.CodeTaskNotFound, rcErr.Code)
	assert.Equal(t, "k", rcErr.IdempotencyKey)
	assert.Len(t, received[*runcoilv1.InvokeRequest](f), 1)
}

func TestInvokeEncodesInputAsJSON(t *testing.T) {
	f := &fakeRuns{invoke: func(int, *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		return &runcoilv1.InvokeResponse{RunId: "run-1"}, nil
	}}
	c := newRunsClient(t, f, nil)

	type input struct {
		AccountID string `json:"account_id"`
	}

	_, err := c.Invoke(context.Background(), runcoil.InvokeRequest{
		WorkerID: "accounts",
		TaskID:   "create-account",
		Input:    input{AccountID: "acct-ada"},
	})
	require.NoError(t, err)

	_, err = c.Invoke(context.Background(), runcoil.InvokeRequest{WorkerID: "accounts", TaskID: "create-account"})
	require.NoError(t, err)

	reqs := received[*runcoilv1.InvokeRequest](f)
	require.Len(t, reqs, 2)
	assert.JSONEq(t, `{"account_id":"acct-ada"}`, string(reqs[0].GetInput()))
	assert.Empty(t, reqs[1].GetInput(), "a nil input sends no bytes")
}

func TestUnencodableInputSendsNothing(t *testing.T) {
	f := &fakeRuns{invoke: func(int, *runcoilv1.InvokeRequest) (*runcoilv1.InvokeResponse, error) {
		return &runcoilv1.InvokeResponse{RunId: "run-1"}, nil
	}}
	c := newRunsClient(t, f, nil)

	_, err := c.Invoke(context.Background(), runcoil.InvokeRequest{
		WorkerID: "accounts",
		TaskID:   "create-account",
		Input:    make(chan int),
	})
	require.ErrorContains(t, err, "invoke: encode input:")

	var rcErr *runcoil.Error
	assert.False(t, errors.As(err, &rcErr), "an encoding error is local, not an answer from the server")
	assert.Empty(t, received[*runcoilv1.InvokeRequest](f))
}

func TestWaitRunCallsAgainAfterTimedOut(t *testing.T) {
	f := &fakeRuns{waitRun: func(n int, _ *runcoilv1.WaitRunRequest) (*runcoilv1.WaitRunResponse, error) {
		switch n {
		case 1:
			return nil, connect.NewError(connect.CodeUnavailable, "server is shutting down")
		case 2:
			return &runcoilv1.WaitRunResponse{
				Run:      &runcoilv1.Run{RunId: "run-1", Status: runcoilv1.RunStatus_RUN_STATUS_RUNNING},
				TimedOut: true,
			}, nil
		default:
			return &runcoilv1.WaitRunResponse{
				Run: &runcoilv1.Run{
					RunId:    "run-1",
					Status:   runcoilv1.RunStatus_RUN_STATUS_SUCCEEDED,
					Terminal: true,
					Output:   []byte(`{"ok":true}`),
				},
			}, nil
		}
	}}
	c := newRunsClient(t, f, nil)

	run, err := c.WaitRun(context.Background(), runcoil.WaitRunRequest{RunID: "run-1", IncludeSteps: true})
	require.NoError(t, err)

	assert.Equal(t, "run-1", run.ID)
	assert.Equal(t, runcoil.RunSucceeded, run.Status)
	assert.True(t, run.Finished)
	assert.JSONEq(t, `{"ok":true}`, string(run.Output))

	reqs := received[*runcoilv1.WaitRunRequest](f)
	require.Len(t, reqs, 3, "one retry of the first call, then the call after timed_out")
	for _, r := range reqs {
		assert.Nil(t, r.GetTimeout(), "no timeout is sent; the server's maximum applies")
		assert.True(t, r.GetIncludeSteps())
	}
}

func TestFailedRunIsReturnedWithoutError(t *testing.T) {
	f := &fakeRuns{getRun: func(int, *runcoilv1.GetRunRequest) (*runcoilv1.GetRunResponse, error) {
		return &runcoilv1.GetRunResponse{Run: &runcoilv1.Run{
			RunId:    "run-1",
			Status:   runcoilv1.RunStatus_RUN_STATUS_FAILED,
			Terminal: true,
			Failure: &runcoilv1.Failure{
				Code:     runcoilv1.FailureCode_FAILURE_CODE_STEP_FAILED,
				Message:  "email is down",
				Attempts: 3,
				StepId:   "send-welcome-email",
			},
		}}, nil
	}}
	c := newRunsClient(t, f, nil)

	run, err := c.GetRun(context.Background(), runcoil.GetRunRequest{RunID: "run-1"})
	require.NoError(t, err)
	assert.Equal(t, runcoil.RunFailed, run.Status)
	assert.Equal(t, &runcoil.Failure{
		Code:     runcoil.FailureStepFailed,
		Message:  "email is down",
		Attempts: 3,
		StepID:   "send-welcome-email",
	}, run.Failure)

	// The same answer through WaitRun is returned with a nil error too.
	waiting := &fakeRuns{waitRun: func(int, *runcoilv1.WaitRunRequest) (*runcoilv1.WaitRunResponse, error) {
		return &runcoilv1.WaitRunResponse{Run: &runcoilv1.Run{
			RunId:    "run-1",
			Status:   runcoilv1.RunStatus_RUN_STATUS_FAILED,
			Terminal: true,
			Failure:  &runcoilv1.Failure{Code: runcoilv1.FailureCode_FAILURE_CODE_ERROR, Message: "boom"},
		}}, nil
	}}
	c = newRunsClient(t, waiting, nil)

	run, err = c.WaitRun(context.Background(), runcoil.WaitRunRequest{RunID: "run-1"})
	require.NoError(t, err)
	assert.Equal(t, runcoil.RunFailed, run.Status)
	require.NotNil(t, run.Failure)
	assert.Equal(t, runcoil.FailureError, run.Failure.Code)
}

func TestGetRunByKeySendsTheKey(t *testing.T) {
	f := &fakeRuns{getRun: func(int, *runcoilv1.GetRunRequest) (*runcoilv1.GetRunResponse, error) {
		return &runcoilv1.GetRunResponse{Run: &runcoilv1.Run{RunId: "run-1"}}, nil
	}}
	c := newRunsClient(t, f, nil)

	run, err := c.GetRun(context.Background(), runcoil.GetRunRequest{
		IdempotencyKey: "onboarding/acct-ada",
		IncludeSteps:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, "run-1", run.ID)

	reqs := received[*runcoilv1.GetRunRequest](f)
	require.Len(t, reqs, 1)
	assert.Equal(t, "onboarding/acct-ada", reqs[0].GetIdempotencyKey())
	assert.Empty(t, reqs[0].GetRunId())
	assert.True(t, reqs[0].GetIncludeSteps())
}

func TestGetRunNeedsExactlyOneOfRunIDAndKey(t *testing.T) {
	cases := []struct {
		name string
		req  runcoil.GetRunRequest
	}{
		{"neither", runcoil.GetRunRequest{}},
		{"both", runcoil.GetRunRequest{RunID: "run-1", IdempotencyKey: "k"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeRuns{}
			c := newRunsClient(t, f, nil)

			_, err := c.GetRun(context.Background(), tc.req)
			require.ErrorContains(t, err, "get run:")
			assert.Empty(t, received[*runcoilv1.GetRunRequest](f))
		})
	}
}

func TestListRunsSendsFiltersAndReturnsNextPage(t *testing.T) {
	after := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	f := &fakeRuns{listRuns: func(int, *runcoilv1.ListRunsRequest) (*runcoilv1.ListRunsResponse, error) {
		return &runcoilv1.ListRunsResponse{
			Runs:          []*runcoilv1.Run{{RunId: "run-2", Status: runcoilv1.RunStatus_RUN_STATUS_FAILED}},
			NextPageToken: "page-2",
		}, nil
	}}
	c := newRunsClient(t, f, nil)

	page, err := c.ListRuns(context.Background(), runcoil.ListRunsRequest{
		WorkerID:     "accounts",
		TaskID:       "create-account",
		Statuses:     []runcoil.RunStatus{runcoil.RunQueued, runcoil.RunFailed},
		CreatedAfter: after,
		PageSize:     20,
		PageToken:    "page-1",
	})
	require.NoError(t, err)

	require.Len(t, page.Runs, 1)
	assert.Equal(t, "run-2", page.Runs[0].ID)
	assert.Equal(t, runcoil.RunFailed, page.Runs[0].Status)
	assert.Equal(t, "page-2", page.NextPageToken)

	reqs := received[*runcoilv1.ListRunsRequest](f)
	require.Len(t, reqs, 1)
	req := reqs[0]
	assert.Equal(t, "accounts", req.GetWorkerId())
	assert.Equal(t, "create-account", req.GetTaskId())
	assert.Equal(t, []runcoilv1.RunStatus{
		runcoilv1.RunStatus_RUN_STATUS_QUEUED,
		runcoilv1.RunStatus_RUN_STATUS_FAILED,
	}, req.GetStatuses())
	assert.True(t, req.GetCreatedAfter().AsTime().Equal(after))
	assert.Nil(t, req.GetCreatedBefore(), "a zero time is not sent")
	assert.Equal(t, uint32(20), req.GetPageSize())
	assert.Equal(t, "page-1", req.GetPageToken())
}

func TestListRunsRejectsLocalErrorsWithoutCalling(t *testing.T) {
	cases := []struct {
		name string
		req  runcoil.ListRunsRequest
		want string
	}{
		{"unknown status", runcoil.ListRunsRequest{Statuses: []runcoil.RunStatus{"paused"}}, "unknown run status"},
		{"unspecified status", runcoil.ListRunsRequest{Statuses: []runcoil.RunStatus{"unspecified"}}, "unknown run status"},
		{"negative page size", runcoil.ListRunsRequest{PageSize: -1}, "page size -1 is out of range"},
		{"page size over uint32", runcoil.ListRunsRequest{PageSize: 1 << 32}, "is out of range"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeRuns{}
			c := newRunsClient(t, f, nil)

			_, err := c.ListRuns(context.Background(), tc.req)
			require.ErrorContains(t, err, "list runs: ")
			assert.ErrorContains(t, err, tc.want)
			assert.Empty(t, received[*runcoilv1.ListRunsRequest](f))
		})
	}
}

func TestRerunSendsFromStepAndKey(t *testing.T) {
	f := &fakeRuns{rerun: func(int, *runcoilv1.RerunRequest) (*runcoilv1.RerunResponse, error) {
		return &runcoilv1.RerunResponse{RunId: "run-2"}, nil
	}}
	c := newRunsClient(t, f, nil)

	inv, err := c.Rerun(context.Background(), runcoil.RerunRequest{
		RunID:          "run-0",
		FromStepID:     "send-welcome-email",
		IdempotencyKey: "rerun/acct-ada/1",
	})
	require.NoError(t, err)
	assert.Equal(t, runcoil.Invocation{RunID: "run-2", IdempotencyKey: "rerun/acct-ada/1"}, inv)

	inv, err = c.Rerun(context.Background(), runcoil.RerunRequest{RunID: "run-0"})
	require.NoError(t, err)
	id, err := uuid.Parse(inv.IdempotencyKey)
	require.NoError(t, err, "a missing key is generated")
	assert.Equal(t, uuid.Version(7), id.Version())

	reqs := received[*runcoilv1.RerunRequest](f)
	require.Len(t, reqs, 2)
	assert.Equal(t, "run-0", reqs[0].GetRunId())
	assert.Equal(t, "send-welcome-email", reqs[0].GetFromStepId())
	assert.Equal(t, "rerun/acct-ada/1", reqs[0].GetIdempotencyKey())
	assert.Empty(t, reqs[1].GetFromStepId(), "an empty step means a rerun from the start")
	assert.Equal(t, inv.IdempotencyKey, reqs[1].GetIdempotencyKey())
}
