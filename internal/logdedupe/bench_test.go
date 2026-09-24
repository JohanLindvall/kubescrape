package logdedupe

import (
	"strconv"
	"testing"
	"time"
)

// BenchmarkAllowNewKeySaturated is the refused-new-key path of a full table
// with a window, which used to walk the whole map under the lock on every call
// (see Table.reclaimAt). It should cost about what an existing key does.
func BenchmarkAllowNewKeySaturated(b *testing.B) {
	for _, n := range []int{64, 1024} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			tab := New(n, 30*time.Minute)
			for i := range n {
				tab.Allow("held-" + strconv.Itoa(i))
			}
			keys := make([]string, 1024)
			for i := range keys {
				keys[i] = "new-" + strconv.Itoa(i)
			}
			i := 0
			for b.Loop() {
				tab.Allow(keys[i%len(keys)])
				i++
			}
		})
	}
}

// BenchmarkAllowExistingKey is the baseline the saturated path is compared to.
func BenchmarkAllowExistingKey(b *testing.B) {
	tab := New(1024, 30*time.Minute)
	for i := range 1024 {
		tab.Allow("held-" + strconv.Itoa(i))
	}
	for b.Loop() {
		tab.Allow("held-7")
	}
}
