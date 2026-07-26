package otlpexport

import (
	"net/http"
	"testing"
)

// OTLP defines 408/429/5xx as RETRYABLE. A collector answering them while it
// accepts other batches is back-pressuring, not rejecting this payload — so
// they must not count as poison evidence and get a good batch dropped.
func TestProbeThrottlingIsNotPoisonEvidence(t *testing.T) {
	for _, code := range []int{408, 429, 500, 502, 503, 504} {
		if respondedError(&HTTPStatusError{Code: code}) {
			t.Errorf("HTTP %d counted as poison evidence; it is retryable back-pressure", code)
		}
	}
	// A genuine rejection still counts.
	for _, code := range []int{400, 401, 403, 404, 413, 422} {
		if !respondedError(&HTTPStatusError{Code: code}) {
			t.Errorf("HTTP %d must count as a collector response", code)
		}
	}
	_ = http.StatusOK
}
