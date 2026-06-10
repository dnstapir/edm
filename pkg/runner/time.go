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

// Unfortunately the hll library does not expose what format
// the HLL is being stored in so figure things out manually.
//
// The format of the bytes are documented at
// https://github.com/aggregateknowledge/hll-storage-spec
//
// See https://github.com/segmentio/go-hll/issues/8 for a request to make this easier.
//
// BEGIN: Code manually based on https://github.com/segmentio/go-hll/blob/main/hll.go

func timeUntilNextMinute() time.Duration {
	return timeUntilNextMinuteFrom(time.Now())
}

func timeUntilNextMinuteFrom(now time.Time) time.Duration {
	return now.Truncate(time.Minute).Add(time.Minute).Sub(now)
}
