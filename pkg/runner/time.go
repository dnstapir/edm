package runner

import "time"

func getStartTimeFromRotationTime(rotationTime time.Time) time.Time {
	// The ticker used to interrupt minimiserLoop is hardcoded to tick at
	// the start of every minute so we can assume the duration we have
	// captured dnstap packets for is 1 minute which should be always true
	// except for the very first collection at startup based on what
	// second the program started, but in that case we just pretend we have
	// the full minute.
	return rotationTime.Add(-time.Second * 60)
}

func intervalStartFromTimes(startTime time.Time, rotationTime time.Time) time.Time {
	if !startTime.IsZero() {
		return startTime
	}
	return getStartTimeFromRotationTime(rotationTime)
}

func timeUntilNextMinute() time.Duration {
	return timeUntilNextMinuteFrom(time.Now())
}

func timeUntilNextMinuteFrom(now time.Time) time.Duration {
	return now.Truncate(time.Minute).Add(time.Minute).Sub(now)
}
