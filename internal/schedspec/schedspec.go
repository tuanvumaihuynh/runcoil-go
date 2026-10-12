// Package schedspec holds the schedule values behind package schedule. It is
// internal so that runcoil can read a schedule's fields when it registers a
// Task, while the fields stay hidden from application code.
package schedspec

import (
	"errors"
	"fmt"
	"strings"
	"time"

	runcoilv1 "github.com/tuanvumaihuynh/runcoil-proto/gen/runcoil/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

type kind int

const (
	kindNone kind = iota
	kindCron
	kindEvery
	kindDaily
)

// Schedule says when the server starts a Run of a Task. The constructors store
// their arguments as they are; Validate checks them when the Task is registered.
type Schedule struct {
	kind        kind
	cron        string
	every       time.Duration
	daily       []Time
	skipOverlap bool
}

// Option changes a Schedule built by Cron or Every.
type Option func(*cfg)

// cfg holds what the options set, until a constructor builds the Schedule.
type cfg struct {
	skipOverlap bool
}

// Time is a time of day in UTC.
type Time struct {
	Hour   int
	Minute int
}

// SkipOverlap returns the option that creates no Run for a tick while the
// previous Run of the schedule is not finished.
func SkipOverlap() Option {
	return func(cfg *cfg) { cfg.skipOverlap = true }
}

// Cron returns a schedule from a five-field cron expression in UTC.
func Cron(expr string, opts ...Option) Schedule {
	cfg := applyOptions(opts)
	return Schedule{kind: kindCron, cron: expr, skipOverlap: cfg.skipOverlap}
}

// Every returns a schedule that ticks every d.
func Every(d time.Duration, opts ...Option) Schedule {
	cfg := applyOptions(opts)
	return Schedule{kind: kindEvery, every: d, skipOverlap: cfg.skipOverlap}
}

// Daily returns a schedule that ticks at each of the given times of day in UTC.
func Daily(at ...Time) Schedule {
	return Schedule{kind: kindDaily, daily: append([]Time(nil), at...)}
}

// At returns the time of day hour:minute in UTC. Validate reports a time outside its range.
func At(hour, minute int) Time {
	return Time{Hour: hour, Minute: minute}
}

// applyOptions runs the options on a fresh cfg. A nil option is skipped.
func applyOptions(opts []Option) cfg {
	var cfg cfg
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return cfg
}

// Validate reports the first problem with s: a zero value, an empty cron
// expression, a TZ= or CRON_TZ= prefix, an Every below one second, a Daily with
// no time, or a time outside its range. It returns nil for a valid schedule.
func Validate(s Schedule) error {
	switch s.kind {
	case kindCron:
		trimmed := strings.TrimSpace(s.cron)
		switch {
		case trimmed == "":
			return errors.New("schedule: cron expression is required")
		case strings.HasPrefix(trimmed, "TZ=") || strings.HasPrefix(trimmed, "CRON_TZ="):
			return errors.New("schedule: time zones are not supported; schedules are read in UTC")
		}

		return nil
	case kindEvery:
		if s.every < time.Second {
			return errors.New("schedule: every must be at least 1s")
		}

		return nil
	case kindDaily:
		if len(s.daily) == 0 {
			return errors.New("schedule: daily needs at least one time")
		}

		for _, t := range s.daily {
			if t.Hour < 0 || t.Hour > 23 {
				return fmt.Errorf("schedule: hour %d is outside 0-23", t.Hour)
			}

			if t.Minute < 0 || t.Minute > 59 {
				return fmt.Errorf("schedule: minute %d is outside 0-59", t.Minute)
			}
		}

		return nil
	default:
		return errors.New("schedule: zero value; build it with Cron, Every, or Daily")
	}
}

// Proto returns the schedule as the message that Register sends. Call it only
// on a schedule that Validate accepted; the zero value has no message and gives nil.
func Proto(s Schedule) *runcoilv1.Schedule {
	switch s.kind {
	case kindCron:
		return &runcoilv1.Schedule{
			Kind:        &runcoilv1.Schedule_Cron{Cron: s.cron},
			SkipOverlap: s.skipOverlap,
		}
	case kindEvery:
		return &runcoilv1.Schedule{
			Kind:        &runcoilv1.Schedule_Every{Every: durationpb.New(s.every)},
			SkipOverlap: s.skipOverlap,
		}
	case kindDaily:
		times := make([]*runcoilv1.TimeOfDay, 0, len(s.daily))
		for _, t := range s.daily {
			times = append(times, &runcoilv1.TimeOfDay{Hour: uint32(t.Hour), Minute: uint32(t.Minute)}) //nolint:gosec // range checked by Validate
		}

		return &runcoilv1.Schedule{
			Kind: &runcoilv1.Schedule_Daily{Daily: &runcoilv1.DailyTimes{At: times}},
		}
	default:
		return nil
	}
}
