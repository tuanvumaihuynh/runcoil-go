// Command invoke calls the create-account Task of the onboarding example. It
// invokes Task "create-account" of worker "accounts", waits for the Run with its
// Steps, and prints the output, or the failure when the Run failed. Run the
// onboarding worker first, in another terminal: this program only invokes the
// Task, and the worker runs it. Start a server, then run with RUNCOIL_ENDPOINT
// and RUNCOIL_TOKEN set (or the defaults).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
)

type onboardingInput struct {
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
}

type onboardingOutput struct {
	AccountID string `json:"account_id"`
	MessageID string `json:"message_id"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := runcoil.NewClient(
		runcoil.Endpoint(envOr("RUNCOIL_ENDPOINT", "http://localhost:7070")),
		runcoil.Token(envOr("RUNCOIL_TOKEN", "dev-token")),
	)
	if err != nil {
		return err
	}

	invocation, err := client.Invoke(ctx, runcoil.InvokeRequest{
		WorkerID: "accounts",
		TaskID:   "create-account",
		Input: onboardingInput{
			AccountID: "acct-ada",
			Name:      "Ada",
			Email:     "ada@example.com",
		},
		IdempotencyKey: "onboarding/acct-ada",
	})
	if err != nil {
		var rcErr *runcoil.Error
		if errors.As(err, &rcErr) && rcErr.Code == runcoil.CodeTaskNotFound {
			return fmt.Errorf("no worker has registered accounts/create-account; run the onboarding example first: %w", err)
		}

		return fmt.Errorf("invoke: %w", err)
	}

	result, err := client.WaitRun(ctx, runcoil.WaitRunRequest{
		RunID:        invocation.RunID,
		IncludeSteps: true,
	})
	if err != nil {
		return fmt.Errorf("wait for run %s: %w", invocation.RunID, err)
	}

	if result.Status == runcoil.RunFailed {
		printFailure(result)
		return fmt.Errorf("run %s failed", result.ID)
	}

	var out onboardingOutput
	if err := json.Unmarshal(result.Output, &out); err != nil {
		return fmt.Errorf("decode output: %w", err)
	}

	fmt.Printf("account %s, welcome email %s\n", out.AccountID, out.MessageID)
	return nil
}

// printFailure prints the code, message, and Step of a failed Run.
func printFailure(r runcoil.Run) {
	fmt.Printf("run %s failed\n", r.ID)

	if r.Failure == nil {
		return
	}

	fmt.Printf("code: %s\n", r.Failure.Code)
	fmt.Printf("message: %s\n", r.Failure.Message)
	if r.Failure.StepID != "" {
		fmt.Printf("step: %s\n", r.Failure.StepID)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
