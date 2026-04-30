package runner

import (
	"testing"
	"time"
)

func TestTimeUntilNextMinuteFrom(t *testing.T) {
	tests := []struct {
		name string
		now  time.Time
		want time.Duration
	}{
		{
			name: "exact minute",
			now:  time.Date(2026, 4, 29, 12, 30, 0, 0, time.UTC),
			want: time.Minute,
		},
		{
			name: "half second after minute",
			now:  time.Date(2026, 4, 29, 12, 30, 0, 500*int(time.Millisecond), time.UTC),
			want: 59*time.Second + 500*time.Millisecond,
		},
		{
			name: "one millisecond before next minute",
			now:  time.Date(2026, 4, 29, 12, 30, 59, int(999*time.Millisecond), time.UTC),
			want: time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := timeUntilNextMinuteFrom(tc.now)
			if got != tc.want {
				t.Fatalf("duration have: %s, want: %s", got, tc.want)
			}
		})
	}
}
