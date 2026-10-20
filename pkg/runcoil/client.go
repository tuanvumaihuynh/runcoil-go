// Package runcoil is the Go SDK for Runcoil. A Client invokes Tasks and reads
// Runs through the server's RunService. A Worker runs the Tasks of one worker
// ID in this process: it registers them with the server, receives their Runs,
// runs the handlers, and reports each result. Create a Client with NewClient,
// then call its methods, or create a Worker with NewWorker, add each Task with
// NewTask, and call Start.
package runcoil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/google/uuid"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1/runcoilv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client is the connection to one server. It is safe for concurrent use. It
// invokes Tasks and reads Runs, and it can serve any number of Workers.
type Client struct {
	logger *slog.Logger
	rpc    runcoilv1connect.WorkerServiceClient
	runs   runcoilv1connect.RunServiceClient
}

// ClientOption configures a Client in NewClient.
type ClientOption func(*clientCfg)

// clientCfg holds what the options set, until NewClient builds the Client.
type clientCfg struct {
	endpoint string
	token    string
	logger   *slog.Logger
}

// NewClient validates the options and returns a Client. It does not contact
// the server; the first call happens in Worker.Start or in a Client method.
func NewClient(opts ...ClientOption) (*Client, error) {
	cfg := clientCfg{logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	// No overall timeout: a Listen stream is open for as long as the worker runs,
	// and a WaitRun call lasts until the Run finishes.
	httpClient := &http.Client{}
	// No read limit: the default is 4 MiB per message, a dispatch can carry more,
	// and the SDK holds no copy of the server's limits.
	transport := connecthttp.NewTransport(httpClient, cfg.endpoint, connecthttp.WithReadMaxBytes(0))
	conn := connect.NewClient(transport, bearer("Bearer "+cfg.token))

	return &Client{
		logger: cfg.logger,
		rpc:    runcoilv1connect.NewWorkerServiceClient(conn),
		runs:   runcoilv1connect.NewRunServiceClient(conn),
	}, nil
}

// validate checks the options that NewClient received. Its errors do not name
// the function; NewClient adds that prefix.
func (cfg *clientCfg) validate() error {
	if cfg.endpoint == "" {
		return errors.New("endpoint is required")
	}

	u, err := url.Parse(cfg.endpoint)
	if err != nil {
		return fmt.Errorf("parse endpoint: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint scheme %q must be http or https", u.Scheme)
	}

	if u.Host == "" {
		return errors.New("endpoint has no host")
	}

	if cfg.token == "" {
		return errors.New("token is required")
	}

	return nil
}

// Endpoint sets the base URL of the server, such as https://runcoil.internal:8080.
// It is required and must use the http or https scheme.
func Endpoint(endpoint string) ClientOption {
	return func(cfg *clientCfg) { cfg.endpoint = endpoint }
}

// Token sets the bearer token that the client sends on every call. It is required.
func Token(token string) ClientOption {
	return func(cfg *clientCfg) { cfg.token = token }
}

// Logger sets the logger for the SDK's own log lines, such as a dispatch that
// could not be acknowledged or a call that is retried. The default discards
// them. A nil logger also discards.
func Logger(l *slog.Logger) ClientOption {
	return func(cfg *clientCfg) {
		if l == nil {
			l = slog.New(slog.DiscardHandler)
		}

		cfg.logger = l
	}
}

// InvokeRequest asks for a Run of a Task.
type InvokeRequest struct {
	WorkerID       string // the worker ID that owns the Task
	TaskID         string // the ID of the Task under its worker ID
	Input          any    // encoded with encoding/json; nil sends no input
	IdempotencyKey string // empty: the SDK generates a UUIDv7
}

// GetRunRequest asks for a snapshot of one Run. Set exactly one of RunID and
// IdempotencyKey.
type GetRunRequest struct {
	RunID          string // the Run ID
	IdempotencyKey string // the key that created the Run
	IncludeSteps   bool   // fill Steps in the answer
}

// WaitRunRequest asks for the Run when it finishes.
type WaitRunRequest struct {
	RunID        string // the Run ID
	IncludeSteps bool   // fill Steps in the answer
}

// ListRunsRequest filters the Runs that ListRuns returns. Every field is
// optional.
type ListRunsRequest struct {
	WorkerID      string      // matches every worker ID when empty
	TaskID        string      // matches every Task ID when empty
	Statuses      []RunStatus // matches every status when empty
	CreatedAfter  time.Time   // no lower bound when zero
	CreatedBefore time.Time   // no upper bound when zero
	PageSize      int         // the server's default when zero; at most math.MaxUint32
	PageToken     string      // the NextPageToken of the previous page
}

// RerunRequest asks for a new Run from a finished Run.
type RerunRequest struct {
	RunID          string // the finished Run to rerun
	FromStepID     string // empty: from the start; otherwise keep the Steps recorded before this one
	IdempotencyKey string // empty: the SDK generates a UUIDv7
}

// Invocation is the answer to a call that creates a Run.
type Invocation struct {
	RunID          string // the Run that the call created, or the Run its key created before
	Deduplicated   bool   // true when the key had already created this Run
	IdempotencyKey string // the key that was sent, given or generated
}

// RunPage is one page of Runs, newest first. The Runs have no Steps, input, or
// output.
type RunPage struct {
	Runs          []Run
	NextPageToken string // empty on the last page
}

// ErrorCode is the Runcoil error code of a failed call. It is empty when the
// server gave no Runcoil answer.
type ErrorCode string

// Error codes that the server answers with.
const (
	CodeInvalidRequest       ErrorCode = "invalid_request"
	CodeUnauthenticated      ErrorCode = "unauthenticated"
	CodeIncompatibleProtocol ErrorCode = "incompatible_protocol"
	CodeTaskNotFound         ErrorCode = "task_not_found"
	CodeRunNotFound          ErrorCode = "run_not_found"
	CodeIdempotencyConflict  ErrorCode = "idempotency_conflict"
	CodeInputTooLarge        ErrorCode = "input_too_large"
	CodeRunNotFinished       ErrorCode = "run_not_finished"
	CodeStepNotFound         ErrorCode = "step_not_found"
)

// Error is a call to the server that failed. Code is the error code of the
// server's answer. It is empty when no answer arrived, for example when the
// context ended while the server was unavailable; then Unwrap returns the
// context's error together with the last transport error. RunID is set for
// CodeIdempotencyConflict and names the Run that holds the key. IdempotencyKey
// is the key that was sent, for Invoke and Rerun.
type Error struct {
	Code           ErrorCode
	Message        string
	RunID          string
	IdempotencyKey string
	err            error
}

// Error returns the code and the message, or only the message when there is no code.
func (e *Error) Error() string {
	if e.Code != "" {
		return string(e.Code) + ": " + e.Message
	}

	return e.Message
}

// Unwrap returns the error of the failed call, so errors.As can reach the
// connect error.
func (e *Error) Unwrap() error {
	return e.err
}

// Invoke creates a Run of the Task and returns once the Run and its idempotency
// key are stored. A worker that has the Task runs the Run later. A repeat with
// the same key and input returns the same Run, with Deduplicated set. A key
// that the SDK generates protects only this call's own retries; to protect a
// trigger that can repeat, give a key derived from the business identity.
func (c *Client) Invoke(ctx context.Context, req InvokeRequest) (Invocation, error) {
	input, err := encodeInput(req.Input)
	if err != nil {
		return Invocation{}, fmt.Errorf("invoke: encode input: %w", err)
	}

	key, err := idempotencyKey(req.IdempotencyKey)
	if err != nil {
		return Invocation{}, fmt.Errorf("invoke: new idempotency key: %w", err)
	}

	pb := &runcoilv1.InvokeRequest{
		WorkerId:       req.WorkerID,
		TaskId:         req.TaskID,
		Input:          input,
		IdempotencyKey: key,
	}

	resp, err := retryRPC(ctx, c.logger, runcoilv1connect.RunServiceInvokeProcedure,
		func() (*runcoilv1.InvokeResponse, error) {
			return c.runs.Invoke(ctx, pb)
		},
	)
	if err != nil {
		return Invocation{}, newError(err, key)
	}

	return Invocation{RunID: resp.GetRunId(), Deduplicated: resp.GetDeduplicated(), IdempotencyKey: key}, nil
}

// GetRun returns a snapshot of one Run, found by its Run ID or by the
// idempotency key that created it. Set exactly one of the two.
func (c *Client) GetRun(ctx context.Context, req GetRunRequest) (Run, error) {
	if (req.RunID == "") == (req.IdempotencyKey == "") {
		return Run{}, errors.New("get run: set exactly one of RunID and IdempotencyKey")
	}

	pb := &runcoilv1.GetRunRequest{IncludeSteps: req.IncludeSteps}
	if req.RunID != "" {
		pb.By = &runcoilv1.GetRunRequest_RunId{RunId: req.RunID}
	} else {
		pb.By = &runcoilv1.GetRunRequest_IdempotencyKey{IdempotencyKey: req.IdempotencyKey}
	}

	resp, err := retryRPC(ctx, c.logger, runcoilv1connect.RunServiceGetRunProcedure,
		func() (*runcoilv1.GetRunResponse, error) {
			return c.runs.GetRun(ctx, pb)
		},
	)
	if err != nil {
		return Run{}, newError(err, "")
	}

	return runFromProto(resp.GetRun()), nil
}

// WaitRun returns the Run once it is finished. The server answers each wait
// after its own limit, so WaitRun asks again until the Run finishes or ctx
// ends. A failed Run is returned with a nil error; check its Status and Failure.
func (c *Client) WaitRun(ctx context.Context, req WaitRunRequest) (Run, error) {
	pb := &runcoilv1.WaitRunRequest{RunId: req.RunID, IncludeSteps: req.IncludeSteps}
	for {
		resp, err := retryRPC(ctx, c.logger, runcoilv1connect.RunServiceWaitRunProcedure,
			func() (*runcoilv1.WaitRunResponse, error) {
				return c.runs.WaitRun(ctx, pb)
			},
		)
		if err != nil {
			return Run{}, newError(err, "")
		}

		// The server ends a wait at its limit with timed_out set; any other answer is final.
		if !resp.GetTimedOut() || resp.GetRun().GetTerminal() {
			return runFromProto(resp.GetRun()), nil
		}
	}
}

// ListRuns returns one page of Runs that match the filters, newest first. Pass
// the NextPageToken of a page as PageToken to read the next one; an empty
// NextPageToken marks the last page.
func (c *Client) ListRuns(ctx context.Context, req ListRunsRequest) (RunPage, error) {
	if req.PageSize < 0 || uint64(req.PageSize) > math.MaxUint32 {
		return RunPage{}, fmt.Errorf("list runs: page size %d is out of range", req.PageSize)
	}

	statuses, err := statusesToProto(req.Statuses)
	if err != nil {
		return RunPage{}, fmt.Errorf("list runs: %w", err)
	}

	pb := &runcoilv1.ListRunsRequest{
		WorkerId:  req.WorkerID,
		TaskId:    req.TaskID,
		Statuses:  statuses,
		PageSize:  uint32(req.PageSize), //nolint:gosec // range checked above
		PageToken: req.PageToken,
	}

	if !req.CreatedAfter.IsZero() {
		pb.CreatedAfter = timestamppb.New(req.CreatedAfter)
	}

	if !req.CreatedBefore.IsZero() {
		pb.CreatedBefore = timestamppb.New(req.CreatedBefore)
	}

	resp, err := retryRPC(ctx, c.logger, runcoilv1connect.RunServiceListRunsProcedure,
		func() (*runcoilv1.ListRunsResponse, error) {
			return c.runs.ListRuns(ctx, pb)
		},
	)
	if err != nil {
		return RunPage{}, newError(err, "")
	}

	page := RunPage{NextPageToken: resp.GetNextPageToken()}
	for _, r := range resp.GetRuns() {
		page.Runs = append(page.Runs, runFromProto(r))
	}

	return page, nil
}

// Rerun creates a new Run from a finished Run: from its start, or from FromStepID
// with the Steps recorded before it. A repeat with the same key returns the same
// Run.
func (c *Client) Rerun(ctx context.Context, req RerunRequest) (Invocation, error) {
	key, err := idempotencyKey(req.IdempotencyKey)
	if err != nil {
		return Invocation{}, fmt.Errorf("rerun: new idempotency key: %w", err)
	}

	pb := &runcoilv1.RerunRequest{
		RunId:          req.RunID,
		FromStepId:     req.FromStepID,
		IdempotencyKey: key,
	}

	resp, err := retryRPC(ctx, c.logger, runcoilv1connect.RunServiceRerunProcedure,
		func() (*runcoilv1.RerunResponse, error) {
			return c.runs.Rerun(ctx, pb)
		},
	)
	if err != nil {
		return Invocation{}, newError(err, key)
	}

	return Invocation{RunID: resp.GetRunId(), Deduplicated: resp.GetDeduplicated(), IdempotencyKey: key}, nil
}

// encodeInput returns the JSON of input, or nil when input is nil.
func encodeInput(input any) ([]byte, error) {
	if input == nil {
		return nil, nil
	}

	return json.Marshal(input)
}

// idempotencyKey returns key, or a new UUIDv7 when key is empty.
func idempotencyKey(key string) (string, error) {
	if key != "" {
		return key, nil
	}

	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}

	return id.String(), nil
}

// statusesToProto maps the statuses of a ListRuns filter to their proto values.
// A status that the proto does not name, UNSPECIFIED included, is an error.
func statusesToProto(statuses []RunStatus) ([]runcoilv1.RunStatus, error) {
	var out []runcoilv1.RunStatus
	for _, s := range statuses {
		v, ok := runcoilv1.RunStatus_value["RUN_STATUS_"+strings.ToUpper(string(s))]
		if !ok || v == 0 {
			return nil, fmt.Errorf("unknown run status %q", s)
		}

		out = append(out, runcoilv1.RunStatus(v))
	}

	return out, nil
}

// retryRPC calls op until it succeeds, until the server answers with a Runcoil
// error, or until ctx ends. A failure with no Runcoil answer is sent again when
// retryable accepts it, after a backoff that starts at retryStart and doubles up
// to retryCap. Each retry logs one debug line. When ctx ends before an answer,
// the error wraps both the context's error and the last failure.
func retryRPC[T any](ctx context.Context, logger *slog.Logger, procedure string, op func() (T, error)) (T, error) {
	var zero T
	backoff := retryStart
	for try := 1; ; try++ {
		answer, err := op()
		if err == nil || errorDetail(err) != nil {
			return answer, err
		}

		if ctx.Err() != nil {
			return zero, fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		}

		if !retryable(err) {
			return zero, err
		}

		logger.DebugContext(ctx, "call failed; retrying",
			slog.String("procedure", procedure),
			slog.Int("try", try),
			slog.Duration("next_delay", backoff),
			slog.Any("error", err),
		)

		if !sleep(ctx, backoff) {
			return zero, fmt.Errorf("%w (last error: %w)", ctx.Err(), err)
		}

		backoff = min(2*backoff, retryCap)
	}
}

// newError returns the *Error for a failed call. key is the idempotency key that
// the call sent, or empty when the call sends none.
func newError(err error, key string) *Error {
	e := &Error{Message: err.Error(), IdempotencyKey: key, err: err}

	d := errorDetail(err)
	if d == nil {
		return e
	}

	e.Code = ErrorCode(enumName(d.GetCode(), "ERROR_CODE_"))
	e.RunID = d.GetRunId()

	var ce *connect.Error
	if errors.As(err, &ce) {
		e.Message = ce.Message()
	}

	return e
}

// errorDetail returns the Runcoil ErrorDetail in the details of err, or nil when
// there is none.
func errorDetail(err error) *runcoilv1.ErrorDetail {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return nil
	}

	for _, d := range ce.Details() {
		msg, err := connectproto.UnmarshalErrorDetail(d)
		if err != nil {
			continue
		}

		if detail, ok := msg.(*runcoilv1.ErrorDetail); ok {
			return detail
		}
	}

	return nil
}

// bearer adds the Authorization header to every call, unary or streaming. The
// header is set on the call's CallInfo before the stream opens, so it goes out
// with the first message.
func bearer(value string) connect.ClientInterceptor {
	return func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			ctx, info := connect.NewClientContext(ctx)
			info.RequestHeader().Set("Authorization", value)
			return next(ctx, spec)
		}
	}
}
