// Command hello runs a worker with worker ID "hello" and one Task, "hello". Start
// a server, then run it with RUNCOIL_ENDPOINT and RUNCOIL_TOKEN set (or the
// defaults). Stop it with Ctrl-C; it drains running handlers for 30 seconds.
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)

type helloInput struct {
	Name string `json:"name"`
}

type helloOutput struct {
	Greeting string `json:"greeting"`
}

type helloTasks struct {
	logger *slog.Logger
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	client, err := runcoil.NewClient(
		runcoil.Endpoint(envOr("RUNCOIL_ENDPOINT", "http://localhost:7070")),
		runcoil.Token(envOr("RUNCOIL_TOKEN", "dev-token")),
		runcoil.Logger(logger),
	)
	if err != nil {
		return err
	}

	worker, err := runcoil.NewWorker(client, runcoil.WorkerID("hello"))
	if err != nil {
		return err
	}

	tasks := &helloTasks{logger: logger}
	if err := registerTasks(worker, tasks); err != nil {
		return err
	}

	if err := worker.Start(ctx); err != nil {
		return err
	}

	logger.Info("worker started", slog.String("worker_id", "hello"))

	<-ctx.Done()

	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := worker.Shutdown(drainCtx); err != nil {
		return err
	}

	logger.Info("worker stopped")
	return nil
}

func registerTasks(worker *runcoil.Worker, tasks *helloTasks) error {
	return worker.NewTask("hello", runcoil.HandlerFunc(tasks.hello))
}

func (t *helloTasks) hello(ctx context.Context, in helloInput) (helloOutput, error) {
	greeting, err := step.Do(ctx, "greet", func(context.Context) (string, error) {
		return "hello, " + in.Name, nil
	})
	if err != nil {
		return helloOutput{}, err
	}

	info := runcoil.Info(ctx)
	t.logger.InfoContext(ctx, "greeted",
		slog.String("run_id", info.RunID),
		slog.Int("attempt", info.Attempt),
		slog.String("name", in.Name),
	)

	return helloOutput{Greeting: greeting}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
