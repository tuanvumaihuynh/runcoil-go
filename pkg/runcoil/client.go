// Package runcoil is the worker side of Runcoil. A worker registers its Tasks
// with the server, receives their Runs, runs the handlers, and reports each
// result. Create a Client with NewClient, a Worker with NewWorker, and then add
// each Task with NewTask before calling Start.
package runcoil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	runcoilv1connect "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1/runcoilv1connect"
)

// Client is the connection to one server. It is safe for concurrent use and
// can serve any number of Workers.
type Client struct {
	logger *slog.Logger
	rpc    runcoilv1connect.WorkerServiceClient
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
// the server; the first call happens in Worker.Start.
func NewClient(opts ...ClientOption) (*Client, error) {
	cfg := clientCfg{logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	// No overall timeout: a Listen stream is open for as long as the worker runs.
	httpClient := &http.Client{}
	// No read limit: the default is 4 MiB per message, a dispatch can carry more,
	// and the SDK holds no copy of the server's limits.
	transport := connecthttp.NewTransport(httpClient, cfg.endpoint, connecthttp.WithReadMaxBytes(0))
	rpc := runcoilv1connect.NewWorkerServiceClient(connect.NewClient(transport, bearer("Bearer "+cfg.token)))

	return &Client{logger: cfg.logger, rpc: rpc}, nil
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

// Token sets the bearer token that the worker sends on every call. It is required.
func Token(token string) ClientOption {
	return func(cfg *clientCfg) { cfg.token = token }
}

// Logger sets the logger for the SDK's own log lines, such as a dispatch that
// could not be acknowledged. The default discards them. A nil logger also discards.
func Logger(l *slog.Logger) ClientOption {
	return func(cfg *clientCfg) {
		if l == nil {
			l = slog.New(slog.DiscardHandler)
		}

		cfg.logger = l
	}
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
