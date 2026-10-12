package step_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tuanvumaihuynh/runcoil-go/internal/runstate"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/runcoil"
	"github.com/tuanvumaihuynh/runcoil-go/pkg/step"
)

func TestDoOutsideRunRunsFunctionOnce(t *testing.T) {
	calls := 0
	got, err := step.Do(context.Background(), "charge", func(context.Context) (string, error) {
		calls++
		return "ch_1", nil
	}, step.Retry(step.RetryPolicy{MaxAttempts: 2}))
	require.NoError(t, err)
	require.Equal(t, "ch_1", got)

	require.Equal(t, 1, calls)
}

func TestDoOutsideRunReturnsErrorAsIs(t *testing.T) {
	want := errors.New("card declined")
	_, err := step.Do(context.Background(), "charge", func(context.Context) (int, error) {
		return 0, want
	})
	require.ErrorIs(t, err, want)
}

// A Step whose MaxAttempts does not fit in the uint32 range is refused before its
// function runs, so the function's side effect cannot happen and then repeat.
func TestDoRejectsMaxAttemptsTooLargeBeforeRunning(t *testing.T) {
	if strconv.IntSize == 32 {
		t.Skip("int is 32 bits, so MaxAttempts cannot exceed the uint32 range")
	}

	ctx := runstate.With(context.Background(), &runstate.State{
		Open: func() bool { return true },
		Report: func(context.Context, runstate.StepReport) (runstate.Record, bool) {
			return runstate.Record{Succeeded: true, Output: []byte(`"ch_1"`)}, true
		},
	})

	ran := false
	_, err := step.Do(ctx, "charge", func(context.Context) (string, error) {
		ran = true
		return "ch_1", nil
	}, step.Retry(step.RetryPolicy{MaxAttempts: math.MaxInt}))
	require.False(t, ran, "function ran; the policy must be refused before the function runs")

	want := fmt.Sprintf("step %q: retry max attempts %d is too large", "charge", math.MaxInt)
	require.EqualError(t, err, want)
}

func TestInfoOutsideRunIsZero(t *testing.T) {
	require.Zero(t, runcoil.Info(context.Background()), "Info outside a Run")
}
