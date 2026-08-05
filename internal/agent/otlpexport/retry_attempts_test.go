package otlpexport

import (
	"context"
	"testing"
	"time"
)

// Retry with attempts <= 0 must still send exactly once: returning nil without
// calling send would report a success for a send that never happened.
func TestRetryAttemptsBelowOneStillSendsOnce(t *testing.T) {
	for _, attempts := range []int{0, -1} {
		calls := 0
		err := Retry(context.Background(), attempts, time.Millisecond, func() error {
			calls++
			return nil
		})
		if err != nil {
			t.Fatalf("attempts=%d: unexpected error %v", attempts, err)
		}
		if calls != 1 {
			t.Fatalf("attempts=%d: send called %d times, want 1", attempts, calls)
		}
	}
}
