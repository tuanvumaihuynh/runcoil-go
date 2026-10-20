package runcoil_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// getRunOf returns the Run that a GetRun call gets from the fake server.
func getRunOf(t *testing.T, run *runcoilv1.Run) runcoil.Run {
	t.Helper()
	f := &fakeRuns{getRun: func(int, *runcoilv1.GetRunRequest) (*runcoilv1.GetRunResponse, error) {
		return &runcoilv1.GetRunResponse{Run: run}, nil
	}}
	c := newRunsClient(t, f, nil)

	got, err := c.GetRun(context.Background(), runcoil.GetRunRequest{RunID: "run-1", IncludeSteps: true})
	require.NoError(t, err)
	return got
}

func TestRunFromProtoKeepsEveryField(t *testing.T) {
	created := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	started := created.Add(time.Second)
	finished := created.Add(time.Minute)
	nextAttempt := created.Add(2 * time.Minute)
	due := created.Add(-time.Second)

	run := getRunOf(t, &runcoilv1.Run{
		RunId:         "run-2",
		WorkerId:      "accounts",
		TaskId:        "create-account",
		Status:        runcoilv1.RunStatus_RUN_STATUS_FAILED,
		QueuedReason:  runcoilv1.QueuedReason_QUEUED_REASON_RETRY_BACKOFF,
		Terminal:      true,
		Source:        runcoilv1.RunSource_RUN_SOURCE_RERUN,
		Attempts:      3,
		Input:         []byte(`{"account_id":"acct-ada"}`),
		Output:        nil,
		CreatedAt:     timestamppb.New(created),
		DueAt:         timestamppb.New(due),
		StartedAt:     timestamppb.New(started),
		FinishedAt:    timestamppb.New(finished),
		NextAttemptAt: timestamppb.New(nextAttempt),
		RerunOf:       "run-1",
		InstanceId:    "instance-7",
		Failure: &runcoilv1.Failure{
			Code:     runcoilv1.FailureCode_FAILURE_CODE_STEP_FAILED,
			Message:  "email is down",
			Attempts: 3,
			StepId:   "send-welcome-email",
		},
		Steps: []*runcoilv1.StepRecord{
			{
				StepId: "create-account",
				Index:  1,
				Status: runcoilv1.StepStatus_STEP_STATUS_SUCCEEDED,
				Output: []byte(`{"id":"acct-ada"}`),
			},
			{
				StepId: "send-welcome-email",
				Index:  2,
				Status: runcoilv1.StepStatus_STEP_STATUS_FAILED,
				Failure: &runcoilv1.Failure{
					Code:     runcoilv1.FailureCode_FAILURE_CODE_ERROR,
					Message:  "smtp refused",
					Attempts: 3,
				},
			},
		},
	})

	assert.Equal(t, runcoil.Run{
		ID:            "run-2",
		WorkerID:      "accounts",
		TaskID:        "create-account",
		Status:        runcoil.RunFailed,
		QueuedReason:  runcoil.QueuedRetryBackoff,
		Finished:      true,
		Source:        runcoil.SourceRerun,
		Attempts:      3,
		Input:         json.RawMessage(`{"account_id":"acct-ada"}`),
		Output:        nil,
		Failure:       &runcoil.Failure{Code: runcoil.FailureStepFailed, Message: "email is down", Attempts: 3, StepID: "send-welcome-email"},
		CreatedAt:     created,
		DueAt:         due,
		StartedAt:     started,
		FinishedAt:    finished,
		NextAttemptAt: nextAttempt,
		RerunOf:       "run-1",
		InstanceID:    "instance-7",
		Steps: []runcoil.StepRecord{
			{ID: "create-account", Index: 1, Status: runcoil.StepSucceeded, Output: json.RawMessage(`{"id":"acct-ada"}`)},
			{
				ID:     "send-welcome-email",
				Index:  2,
				Status: runcoil.StepFailed,
				Failure: &runcoil.Failure{
					Code:     runcoil.FailureError,
					Message:  "smtp refused",
					Attempts: 3,
				},
			},
		},
	}, run)
}

func TestRunFromProtoZeroesUnsetFields(t *testing.T) {
	run := getRunOf(t, &runcoilv1.Run{
		RunId:  "run-1",
		Status: runcoilv1.RunStatus_RUN_STATUS_QUEUED,
	})

	assert.Equal(t, runcoil.RunQueued, run.Status)
	assert.Empty(t, run.QueuedReason, "an unset reason is empty")
	assert.Empty(t, run.Source)
	assert.True(t, run.CreatedAt.IsZero())
	assert.True(t, run.DueAt.IsZero())
	assert.True(t, run.StartedAt.IsZero())
	assert.True(t, run.FinishedAt.IsZero())
	assert.True(t, run.NextAttemptAt.IsZero())
	assert.Nil(t, run.Input, "empty bytes are a nil RawMessage")
	assert.Nil(t, run.Output)
	assert.Nil(t, run.Failure)
	assert.Nil(t, run.Steps)
}

func TestEnumValueOutsideKnownSetIsItsNumber(t *testing.T) {
	run := getRunOf(t, &runcoilv1.Run{
		RunId:        "run-1",
		Status:       runcoilv1.RunStatus(99),
		QueuedReason: runcoilv1.QueuedReason(42),
		Source:       runcoilv1.RunSource(7),
		Failure:      &runcoilv1.Failure{Code: runcoilv1.FailureCode(55)},
		Steps: []*runcoilv1.StepRecord{
			{StepId: "charge", Status: runcoilv1.StepStatus(8)},
		},
	})

	assert.Equal(t, runcoil.RunStatus("99"), run.Status)
	assert.Equal(t, runcoil.QueuedReason("42"), run.QueuedReason)
	assert.Equal(t, runcoil.RunSource("7"), run.Source)
	require.NotNil(t, run.Failure)
	assert.Equal(t, runcoil.FailureCode("55"), run.Failure.Code)
	require.Len(t, run.Steps, 1)
	assert.Equal(t, runcoil.StepStatus("8"), run.Steps[0].Status)
}

func TestErrorCodeIsLowerCaseNameWithoutPrefix(t *testing.T) {
	f := &fakeRuns{getRun: func(int, *runcoilv1.GetRunRequest) (*runcoilv1.GetRunResponse, error) {
		return nil, runcoilErr(t, connect.CodeNotFound, "no run", &runcoilv1.ErrorDetail{
			Code: runcoilv1.ErrorCode_ERROR_CODE_RUN_NOT_FOUND,
		})
	}}
	c := newRunsClient(t, f, nil)

	_, err := c.GetRun(context.Background(), runcoil.GetRunRequest{RunID: "missing"})
	require.Error(t, err)
	assert.Equal(t, "run_not_found: no run", err.Error())
}
