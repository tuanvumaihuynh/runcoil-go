package runcoil

import "time"

// SetReminderEvery sets how often an outage repeats its warning, for a test. It
// returns the function that restores the previous interval.
func SetReminderEvery(d time.Duration) (restore func()) {
	old := reminderEvery
	reminderEvery = d
	return func() { reminderEvery = old }
}
