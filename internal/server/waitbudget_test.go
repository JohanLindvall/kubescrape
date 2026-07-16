package server

import (
	"net/http/httptest"
	"testing"
	"time"
)

// A plain-integer ?wait= with a SUB-SECOND MaxWait must clamp to MaxWait, not
// truncate to zero whole seconds (which silently made the lookup non-blocking).
func TestWaitBudgetSubSecondMaxWait(t *testing.T) {
	s := New(Config{MaxWait: 700 * time.Millisecond})
	for _, tc := range []struct {
		q    string
		want time.Duration
	}{
		{"?wait=1", 700 * time.Millisecond},              // integer form clamps to MaxWait
		{"?wait=1s", 700 * time.Millisecond},             // duration form clamps identically
		{"?wait=200ms", 200 * time.Millisecond},          // shorter than MaxWait honored
		{"?wait=99999999999999", 700 * time.Millisecond}, // overflow-guard path
		{"", 700 * time.Millisecond},                     // default = MaxWait
	} {
		r := httptest.NewRequest("GET", "/v1/containers/x"+tc.q, nil)
		got, err := s.waitBudget(r)
		if err != nil {
			t.Fatalf("%q: %v", tc.q, err)
		}
		if got != tc.want {
			t.Fatalf("%q: budget = %v, want %v", tc.q, got, tc.want)
		}
	}
}
