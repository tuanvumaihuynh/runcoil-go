// Package schedule builds the schedules that start the Runs of a Task. Pass a
// schedule to runcoil.Schedule when the Task is registered. Schedules are read
// in UTC, and the server computes the ticks.
package schedule

import (
	"time"

	"github.com/tuanvumaihuynh/runcoil-go/internal/schedspec"
)

// Schedule says when the server starts a Run of a Task. Build it with Cron,
// Every, or Daily. Schedules are read in UTC. NewTask reports an invalid
// argument: an empty or zero value, an Every below 1 second, a time outside its
// range, or a TZ= or CRON_TZ= prefix. Start reports a cron expression the
// server cannot parse, because only the server parses cron.
type Schedule = schedspec.Schedule

// Option changes a schedule built by Cron or Every. Build one with SkipOverlap.
type Option = schedspec.Option

// Time is a time of day in UTC. Build one with At.
type Time = schedspec.Time

// Cron returns a schedule from a five-field cron expression in UTC, such as
// "0 9 * * *" for every day at 09:00. Use it for calendar-based times. The
// expression must not start with a TZ= or CRON_TZ= prefix. An expression the
// server cannot parse is reported by Start.
func Cron(expr string, opts ...Option) Schedule {
	return schedspec.Cron(expr, opts...)
}

// Every returns a schedule that ticks every d, which must be at least 1 second.
// Ticks are multiples of d counted from the Unix epoch, in UTC. An interval that
// does not divide 24 hours therefore falls at a different time of day on each
// day, so use Cron or Daily for a fixed time of day.
func Every(d time.Duration, opts ...Option) Schedule {
	return schedspec.Every(d, opts...)
}

// Daily returns a schedule that ticks at each given time of day in UTC. Use it
// for a few fixed times; at least one time is required.
func Daily(at ...Time) Schedule {
	return schedspec.Daily(at...)
}

// At returns the time of day hour:minute in UTC, for Daily. The hour is 0-23
// and the minute is 0-59; NewTask rejects an hour or minute outside those ranges.
func At(hour, minute int) Time {
	return schedspec.At(hour, minute)
}

// SkipOverlap is an option for Cron and Every. At a tick while the previous Run
// of the schedule is not finished, it creates no Run for that tick. Without it,
// a new Run starts beside the old one.
func SkipOverlap() Option {
	return schedspec.SkipOverlap()
}
