package bearer

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// readErrors reads one role's counter. Package-level state is shared across
// tests in this binary, so every assertion is a DELTA around the action.
func readErrors(t *testing.T, role string) float64 {
	t.Helper()
	return obs.BearerTokenReadErrors.WithLabelValues(role).Value()
}
