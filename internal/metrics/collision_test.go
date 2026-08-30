package metrics

import (
	"testing"
	"time"

	"github.com/JohanLindvall/haste/xxh3"
)

// series.db is bucketed by the LOW HALF of the 128-bit series key, with the
// full key kept on the sample and the rare collision chained. That chain is
// the entire reason the narrow bucket does not narrow IDENTITY, and no input a
// test can write reaches it — two label combinations agreeing on 64 hash bits
// is the event the width exists to make impossible. So the chain is exercised
// here through link/find/unlink with hand-made colliding keys: without these
// tests the correctness argument for the map's shape is unexecuted code.

// collide builds n keys sharing a low half.
func collide(n int) []xxh3.Uint128 {
	out := make([]xxh3.Uint128, n)
	for i := range out {
		out[i] = xxh3.Uint128{Hi: uint64(i) + 1, Lo: 0xDEADBEEFCAFEBABE}
	}
	return out
}

func linkSample(t *testing.T, s *series, key xxh3.Uint128, labels string, value float64) *expiringSample {
	t.Helper()
	samp := &expiringSample{sample: sample{labels: labels, value: value}, when: s.epoch()}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.link(samp, key)
	return samp
}

func TestCollidingKeysStayDistinctSeries(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_000, 0))
	defer testEpoch.Store(0)
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 10, expiration: time.Hour})
	keys := collide(3)
	for i, k := range keys {
		linkSample(t, s, k, `{i="`+string(rune('a'+i))+`"}`, float64(i+1))
	}
	if s.count != 3 {
		t.Fatalf("count = %d, want 3 (a chain of three is one map entry and three series)", s.count)
	}
	if n := len(s.db); n != 1 {
		t.Fatalf("len(db) = %d, want 1 — the three share a bucket", n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, k := range keys {
		got := s.find(k)
		if got == nil {
			t.Fatalf("find(%v) missed a chained sample", k)
		}
		if got.value != float64(i+1) {
			t.Fatalf("find(%v) returned the wrong sample: value %v, want %v", k, got.value, i+1)
		}
	}
	// A key that merely shares the low half resolves to nothing, which is the
	// property that makes the bucket an index rather than the identity.
	if got := s.find(xxh3.Uint128{Hi: 99, Lo: keys[0].Lo}); got != nil {
		t.Fatalf("find returned %v for a key no sample holds", got.labels)
	}
	var walked int
	for range s.all() {
		walked++
	}
	if walked != 3 {
		t.Fatalf("all() walked %d samples, want 3", walked)
	}
}

func TestUnlinkKeepsTheRestOfTheChain(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_000, 0))
	defer testEpoch.Store(0)
	keys := collide(3)
	// Every position: the head (inserted last), the middle, the tail.
	for _, drop := range []int{0, 1, 2} {
		s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 10, expiration: time.Hour})
		for i, k := range keys {
			linkSample(t, s, k, `{i="`+string(rune('a'+i))+`"}`, float64(i+1))
		}
		s.mu.Lock()
		s.unlink(s.find(keys[drop]))
		if s.count != 2 {
			t.Fatalf("drop %d: count = %d, want 2", drop, s.count)
		}
		for i, k := range keys {
			got := s.find(k)
			if i == drop {
				if got != nil {
					t.Fatalf("drop %d: the unlinked sample is still findable", drop)
				}
				continue
			}
			if got == nil || got.value != float64(i+1) {
				t.Fatalf("drop %d: sample %d was lost with its neighbour", drop, i)
			}
		}
		if drop == 0 && len(s.db) != 1 {
			t.Fatalf("drop %d: the bucket must survive while the chain does", drop)
		}
		s.mu.Unlock()
	}
	// Emptying the chain removes the bucket, so a series that churns keys does
	// not retain a map entry per bucket it ever used.
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 10, expiration: time.Hour})
	for i, k := range keys {
		linkSample(t, s, k, `{i="`+string(rune('a'+i))+`"}`, float64(i+1))
	}
	s.mu.Lock()
	for _, k := range keys {
		s.unlink(s.find(k))
	}
	if len(s.db) != 0 || s.count != 0 {
		t.Fatalf("after unlinking every sample: len(db)=%d count=%d, want 0/0", len(s.db), s.count)
	}
	s.mu.Unlock()
}

// snapshot unlinks the sample it is looking at (the grace-period delete) while
// all() is walking the chain, so all() must read next BEFORE it yields. Read
// after, the walk ends at the first expired sample and every sample behind it
// in the bucket silently stops being exported — a data loss that only a
// collision could ever expose.
func TestExpiryOfAChainedSampleDoesNotStrandItsNeighbours(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_000, 0))
	defer testEpoch.Store(0)
	s := newTestSeries(seriesSpec{name: "c", kind: kindGauge, maxSize: 10, expiration: time.Minute})
	keys := collide(3)
	for i, k := range keys {
		samp := linkSample(t, s, k, `{i="`+string(rune('a'+i))+`"}`, float64(i+1))
		samp.exported = true
		if i == 2 {
			// The HEAD of the chain (linked last) is the stale one.
			samp.when = s.epoch() - int64((10 * time.Minute).Seconds())
		}
	}
	got := s.snapshot()
	if len(got) != 2 {
		t.Fatalf("snapshot returned %d samples, want the two live ones", len(got))
	}
	if s.count != 2 {
		t.Fatalf("count = %d after one expiry, want 2", s.count)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range 2 {
		if s.find(keys[i]) == nil {
			t.Fatalf("live sample %d was dropped when its chain neighbour expired", i)
		}
	}
	if s.find(keys[2]) != nil {
		t.Fatal("the expired sample is still in the map")
	}
}

// The cardinality cap counts SERIES, and a chain is several series in one map
// entry — so the cap must read count, never len(db).
func TestCardinalityCapCountsChainedSeries(t *testing.T) {
	setTimeForTest(time.Unix(1_700_000_000, 0))
	defer testEpoch.Store(0)
	s := newTestSeries(seriesSpec{name: "c", kind: kindCounter, maxSize: 2, expiration: time.Hour})
	keys := collide(3)
	s.mu.Lock()
	for i, k := range keys {
		if samp := s.admit(k, labels{{"i", string(rune('a' + i))}}, s.epoch(), emptyResource, nil); (samp == nil) != (i == 2) {
			t.Fatalf("admit %d: got %v, want the third to be refused by the cap", i, samp != nil)
		}
	}
	s.mu.Unlock()
	if s.count != 2 {
		t.Fatalf("count = %d, want 2", s.count)
	}
	if s.drops.Capped() != 1 {
		t.Fatalf("capped drops = %d, want 1", s.drops.Capped())
	}
}
