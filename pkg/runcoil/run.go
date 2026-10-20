package runcoil

import (
	"encoding/json"
	"strings"
	"time"

	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Run is a snapshot of one Run of a Task. A time is zero when the server has
// none, and Input and Output are nil when they are empty.
type Run struct {
	ID            string          // the Run ID
	WorkerID      string          // the worker ID that owns the Task
	TaskID        string          // the ID of the Task under its worker ID
	Status        RunStatus       // where the Run stands
	QueuedReason  QueuedReason    // why the Run is queued; set while Status is RunQueued
	Finished      bool            // true when Status will not change
	Source        RunSource       // what created the Run
	Attempts      int             // attempts started so far
	Input         json.RawMessage // the JSON input
	Output        json.RawMessage // the JSON output of a succeeded Run
	Failure       *Failure        // set when the Run failed
	CreatedAt     time.Time       // when the Run was created
	DueAt         time.Time       // the schedule's tick, or the time of acceptance
	StartedAt     time.Time       // when the first attempt was acknowledged by a worker
	FinishedAt    time.Time       // when the Run finished
	NextAttemptAt time.Time       // the start of the next attempt, while queued for a retry
	RerunOf       string          // the Run this one reruns
	InstanceID    string          // the instance that holds the attempt
	Steps         []StepRecord    // set when the request asks for Steps
}

// Failure is the final error of a Step or a Run. Only its text survives the server.
type Failure struct {
	Code     FailureCode
	Message  string
	Attempts int    // attempts used; for a Run, the attempts of the Run
	StepID   string // for a Run that failed in a Step, that Step
}

// StepRecord is the stored outcome of one Step of a Run. A repeated Step ID
// arrives numbered, as in "charge#2".
type StepRecord struct {
	ID      string
	Index   int             // order of commit within the Run, from 1
	Status  StepStatus      // how the Step ended
	Output  json.RawMessage // the JSON output, set when the Step succeeded
	Failure *Failure        // set when the Step failed for good
}

// RunStatus is where a Run stands.
type RunStatus string

// Run statuses.
const (
	RunQueued    RunStatus = "queued"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// QueuedReason says why a queued Run is waiting.
type QueuedReason string

// Reasons a Run is queued.
const (
	QueuedWaitingForWorker   QueuedReason = "waiting_for_worker"
	QueuedWaitingForCapacity QueuedReason = "waiting_for_capacity"
	QueuedRetryBackoff       QueuedReason = "retry_backoff"
)

// RunSource says what created a Run.
type RunSource string

// Sources of a Run.
const (
	SourceInvoke   RunSource = "invoke"
	SourceSchedule RunSource = "schedule"
	SourceRerun    RunSource = "rerun"
)

// StepStatus is the final outcome of a Step.
type StepStatus string

// Final outcomes of a Step.
const (
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
)

// FailureCode says why a Step or a Run failed for good.
type FailureCode string

// Reasons a Step or a Run failed for good.
const (
	FailureError              FailureCode = "error"
	FailurePanic              FailureCode = "panic"
	FailureStepFailed         FailureCode = "step_failed"
	FailureAttemptLost        FailureCode = "attempt_lost"
	FailureOutputNotEncodable FailureCode = "output_not_encodable"
	FailureOutputTooLarge     FailureCode = "output_too_large"
	FailureRunDataLimit       FailureCode = "run_data_limit"
	FailureWorkerWaitLimit    FailureCode = "worker_wait_limit"
	FailureStartDeadline      FailureCode = "start_deadline"
)

// protoEnum is a generated proto enum: an int32 whose String method returns the
// proto name of its value.
type protoEnum interface {
	~int32
	String() string
}

// enumName returns the proto name of v without its prefix, in lower case, such
// as "queued" for RUN_STATUS_QUEUED. The UNSPECIFIED value maps to "". A value
// that the proto does not name has no name, so its String is its number.
func enumName[E protoEnum](v E, prefix string) string {
	name := v.String()
	if name == prefix+"UNSPECIFIED" {
		return ""
	}

	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

// runFromProto converts a Run from the server. A nil timestamp is the zero time.
func runFromProto(p *runcoilv1.Run) Run {
	r := Run{
		ID:            p.GetRunId(),
		WorkerID:      p.GetWorkerId(),
		TaskID:        p.GetTaskId(),
		Status:        RunStatus(enumName(p.GetStatus(), "RUN_STATUS_")),
		QueuedReason:  QueuedReason(enumName(p.GetQueuedReason(), "QUEUED_REASON_")),
		Finished:      p.GetTerminal(),
		Source:        RunSource(enumName(p.GetSource(), "RUN_SOURCE_")),
		Attempts:      int(p.GetAttempts()),
		Input:         bytesFromProto(p.GetInput()),
		Output:        bytesFromProto(p.GetOutput()),
		Failure:       failureFromProto(p.GetFailure()),
		CreatedAt:     timeFromProto(p.GetCreatedAt()),
		DueAt:         timeFromProto(p.GetDueAt()),
		StartedAt:     timeFromProto(p.GetStartedAt()),
		FinishedAt:    timeFromProto(p.GetFinishedAt()),
		NextAttemptAt: timeFromProto(p.GetNextAttemptAt()),
		RerunOf:       p.GetRerunOf(),
		InstanceID:    p.GetInstanceId(),
	}

	for _, s := range p.GetSteps() {
		r.Steps = append(r.Steps, stepFromProto(s))
	}

	return r
}

// stepFromProto converts the record of one Step.
func stepFromProto(p *runcoilv1.StepRecord) StepRecord {
	return StepRecord{
		ID:      p.GetStepId(),
		Index:   int(p.GetIndex()),
		Status:  StepStatus(enumName(p.GetStatus(), "STEP_STATUS_")),
		Output:  bytesFromProto(p.GetOutput()),
		Failure: failureFromProto(p.GetFailure()),
	}
}

// failureFromProto converts a failure, or returns nil when there is none.
func failureFromProto(p *runcoilv1.Failure) *Failure {
	if p == nil {
		return nil
	}

	return &Failure{
		Code:     FailureCode(enumName(p.GetCode(), "FAILURE_CODE_")),
		Message:  p.GetMessage(),
		Attempts: int(p.GetAttempts()),
		StepID:   p.GetStepId(),
	}
}

// bytesFromProto returns the bytes as JSON, or nil when they are empty.
func bytesFromProto(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}

	return json.RawMessage(b)
}

// timeFromProto returns the time of a timestamp, or the zero time when it is nil.
func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}

	return ts.AsTime()
}
