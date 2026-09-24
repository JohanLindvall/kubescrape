package metaclient

// The response cache: the entry (cacheEntry), the hit path (lookupEntry), the
// idle sweep and hard-cap trim (evictLocked) with their report, the periodic
// Debug summary, and the Cache-Control reading (maxAge) that decides whether a
// 200 is cached at all. The request path that fills it is fetch, in client.go.

import (
	"context"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// cacheReportInterval paces the Debug cache summary. The question it answers —
// "is the ETag path working, or is every lookup a full body?" — is a RATIO, so
// one line a minute carrying the cumulative tallies is strictly more useful
// than a line per request.
const cacheReportInterval = time.Minute

// reportCache writes the periodic Debug summary of the response cache.
//
// Logger.Enabled is checked BEFORE the throttle, and the order is load-bearing
// because this runs once per lookup on the concurrent ingest and cadvisor
// paths. It used to be the other way round, on the stated grounds that "Allow
// is a single atomic load while Enabled is an interface call" — which is not
// what allow does: its FIRST statement is a clock read
// (time.Since(throttleEpoch), the runtime's monotonic nanotime), and a clock
// read is the expensive half. The figures behind the swap were taken when that
// read was still time.Now().UnixNano(), which the monotonic read replaced:
// time.Now().UnixNano() 57.9 ns, the whole throttle 63.3 ns, Logger.Enabled
// 8.3 ns, serially on this repo's reference machine (and 27.5 vs 10.6 ns/op at
// 4 procs). They are not a measurement of today's allow; the ordering does not
// rest on them, only on a clock read costing more than Enabled, and on the
// behaviour below.
//
// It is also better on the behaviour the old order apologised for: no throttle
// slot is spent while Debug is off, so raising the level mid-incident produces
// a summary on the next lookup instead of up to a minute later.
func (c *Client) reportCache() {
	log := c.logger()
	if !log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	if !c.cacheReport.allow(cacheReportInterval) {
		return
	}
	c.mu.RLock()
	entries := len(c.cache)
	c.mu.RUnlock()
	// Cumulative, not per-window: two consecutive lines differ by the window,
	// and a cumulative number cannot be misread as a rate.
	log.Debug("metadata cache", "entries", entries, "maxEntries", maxCacheEntries,
		"hits", c.hits.Load(), "notModified", c.revalidated.Load(), "fetched", c.fetched.Load(),
		"notFound", c.missing.Load(), "errors", c.failed.Load())
}

type cacheEntry struct {
	// decoded is the response body unmarshaled once into the caller's result
	// type (a pointer), stored so cache hits and 304s skip the JSON decode. It
	// is never mutated after storing; hits receive a SHALLOW copy — maps/slices
	// are shared under the same treat-as-immutable contract the store uses.
	// The raw body is NOT retained: every caller of a given URL asks for the
	// same result type, so the decoded value serves every hit and a body kept
	// beside it was ~18% of the cache's footprint for nothing (an entry that
	// cannot be copied out is dropped and re-fetched — see lookupEntry).
	decoded any
	etag    string
	expires time.Time
	// used is when this entry was last READ or written. Eviction sweeps on
	// it, not on expires: an EXPIRED entry is exactly the state If-None-Match
	// is built on — it still carries the ETag that turns the next read into a
	// 304 — so sweeping by expiry threw away the revalidation state for every
	// URL polled slower than the sweep (the 1m self and node-metadata reads
	// against a 10s TTL), and each of those re-fetched a full body forever.
	// What the sweep is actually for is the dead container nobody asks about
	// again, and that is idleness.
	used time.Time
}

// lookupEntry reads the cache under the lock, classifying the entry as fresh
// (serve locally), stale-but-present (revalidate with If-None-Match), or
// absent. An entry whose decoded value is not v's type is dropped and reported
// absent, so this call re-fetches it from scratch: the cache holds one decoded
// value per URL and every caller of an endpoint asks for the same type, making
// that unreachable in practice — but a value that cannot be copied out must
// never be served, and dropping it also re-populates the entry usefully.
func (c *Client) lookupEntry(key string, v any) (entry cacheEntry, cached, fresh bool) {
	now := c.now()
	c.mu.RLock()
	entry, cached = c.cache[key]
	c.mu.RUnlock()
	if !cached {
		return cacheEntry{}, false, false
	}
	if !sameType(v, entry.decoded) {
		c.mu.Lock()
		// Re-check under the write lock: another goroutine may have replaced the
		// entry with one this caller CAN use while the lock was released, and
		// dropping that would throw away a good 200.
		if cur, ok := c.cache[key]; ok && !sameType(v, cur.decoded) {
			delete(c.cache, key)
		}
		c.mu.Unlock()
		return cacheEntry{}, false, false
	}
	// The stamp feeds eviction and nothing else, against a 5-minute idle window,
	// so it is refreshed COARSELY: writing it on every hit is what forced the
	// exclusive lock onto the read path, and an entry read at all is re-stamped
	// within a fraction of the window either way. (It also keeps a
	// revalidated-but-stale entry alive, which is why it is not gated on
	// freshness.)
	if now.Sub(entry.used) >= usedStampGranularity {
		c.mu.Lock()
		if cur, ok := c.cache[key]; ok && cur.used.Equal(entry.used) {
			cur.used = now
			c.cache[key] = cur
		}
		c.mu.Unlock()
		entry.used = now
	}
	return entry, true, now.Before(entry.expires)
}

// usedStampGranularity is how stale an entry's idle stamp may get before a hit
// refreshes it. Small against cacheMaxIdle (so a live entry is never swept) and
// large against a lookup (so the refresh is rare enough that the hit path is a
// read-lock acquisition and nothing more).
const usedStampGranularity = cacheMaxIdle / 10

// cacheSweepEvery is how often IDLE entries are swept below the cap.
const cacheSweepEvery = time.Minute

// cacheMaxIdle is how long an unread entry is kept. Comfortably above the
// slowest poll any caller makes (the 1m self-attributes and node-metadata
// refreshes), so a periodic reader always still finds its ETag and gets a
// 304 instead of a full body; a container nobody looks up again is gone
// within two sweeps of it.
const cacheMaxIdle = 5 * time.Minute

// maxCacheEntries bounds the response cache. Without a cap the map grows by
// one entry per distinct container/pod URL ever fetched — a steady leak on
// nodes with pod churn (dead containers are never requested again).
const maxCacheEntries = 4096

// evictLowWater is the size eviction trims down to once the cap is exceeded.
// Trimming below the cap (rather than to it) amortizes the two O(n) map sweeps
// over ~1000 inserts instead of running them on every insert while full — this
// matters because the sweeps hold the mutex shared across concurrent ingest and
// cadvisor lookups.
const evictLowWater = maxCacheEntries * 3 / 4

// evictLocked trims the cache when it exceeds the cap: IDLE entries first,
// then arbitrary ones. An arbitrary eviction discards the ETag with the
// entry, so that URL's next lookup is a full 200 re-fetch, not a cheap
// revalidation — the price of the hard bound, paid only when 4096+ distinct
// URLs are genuinely live. Caller holds the mutex.
//
// It DECIDES and returns; it does not log. Everything here runs under the
// exclusive lock shared by every concurrent lookup, so the report is the
// caller's to emit once the lock is gone (see reportEviction). dropped is how
// many entries the ARBITRARY trim discarded, held how many remain.
func (c *Client) evictLocked() (dropped, held int) {
	now := c.now()
	// Sweep idle entries periodically even below the cap. The cap alone
	// only ran at 4096 entries, so on a node with pod churn the cache held
	// thousands of dead containers' FULL pod documents — tens of MB of heap
	// that nothing would ever ask for again — until the next insert crossed the
	// threshold. Over the cap the same sweep runs unconditionally, on the way
	// to the trim below, and resets the cadence all the same.
	over := len(c.cache) > maxCacheEntries
	if !over && now.Sub(c.lastSweep) < cacheSweepEvery {
		return 0, len(c.cache)
	}
	c.lastSweep = now
	for k, e := range c.cache {
		if now.Sub(e.used) > cacheMaxIdle {
			delete(c.cache, k)
		}
	}
	if !over {
		return 0, len(c.cache)
	}
	before := len(c.cache)
	for k := range c.cache {
		if len(c.cache) <= evictLowWater {
			break
		}
		delete(c.cache, k)
	}
	return before - len(c.cache), len(c.cache)
}

// reportEviction warns that the ARBITRARY trim ran, which has a cost nothing
// else reports: an entry evicted that way takes its ETag with it, so that
// URL's next lookup is a full 200 instead of a 304. On a node whose live URL
// count sits above the cap that is a permanent state — every lookup re-fetches
// a full body from the singleton — and the only symptom is load on the
// metadata service. Throttled, because it then happens on roughly every
// insert.
//
// Called with c.mu NOT held; see the call site.
func (c *Client) reportEviction(dropped, held int) {
	if dropped <= 0 || !c.evictWarn.allow(warnInterval) {
		return
	}
	c.logger().Warn("the metadata response cache is at its hard cap; evicted entries lose their ETag, so their next lookup re-fetches a full body",
		"dropped", dropped, "entries", held, "maxEntries", maxCacheEntries)
}

// sameType reports whether a cached decoded value can be copied into dst: both
// must be pointers to the same type.
func sameType(dst, src any) bool {
	if src == nil {
		return false
	}
	dv, sv := reflect.ValueOf(dst), reflect.ValueOf(src)
	return dv.Kind() == reflect.Pointer && dv.Type() == sv.Type()
}

// copyDecoded sets *dst = *src. The copy is shallow: maps and slices stay
// shared with the cached value, which is never mutated (the store's
// shallow-copy contract). Types are checked by lookupEntry.
func copyDecoded(dst, src any) {
	reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(src).Elem())
}

// maxCacheTTL caps the freshness lifetime this client will honour. The header
// is the SERVER's, and time.Duration is int64 NANOseconds: 1.85e10 seconds
// overflows it, so an unclamped `secs * time.Second` turns a large max-age into
// either a ~49-year TTL (the entry is served forever, never revalidated, and
// the idle sweep cannot reclaim it while something keeps reading it) or a
// NEGATIVE one (caching silently off — the opposite of what the header asked
// for). Clamping is not only overflow insurance: no metadata document is worth
// holding for a day, and a header past this is a misdirected or hostile
// endpoint, which the response-size and ETag paths already guard against.
const maxCacheTTL = 24 * time.Hour

// maxAge extracts the Cache-Control max-age; zero when absent or unparseable,
// and zero whenever no-store or no-cache is also present — either directive
// forbids serving a stored response without asking again, and this client's
// only storage is its cache, so both mean "do not cache". kubescrape's own
// server never combines them with a max-age, but this package is public.
//
// That promise holds for every spelling RFC 9111 allows, which is wider than
// the one kubescrape's server writes: directive names are CASE-INSENSITIVE
// (§5.2), and a Cache-Control header repeated on several lines is ONE list
// (RFC 9110 §5.3). Reading only the first line with exact compares cached
// `No-Store, max-age=60` for a minute — the unsafe direction — and let a proxy
// that appends `no-store` as its own header line be ignored.
//
// The value is clamped to maxCacheTTL BEFORE the multiply (see the constant).
func maxAge(resp *http.Response) time.Duration {
	var ttl time.Duration
	for _, line := range resp.Header.Values("Cache-Control") {
		for part := range strings.SplitSeq(line, ",") {
			part = strings.TrimSpace(part)
			if strings.EqualFold(part, "no-store") || strings.EqualFold(part, "no-cache") {
				return 0
			}
			const prefix = "max-age="
			if len(part) <= len(prefix) || !strings.EqualFold(part[:len(prefix)], prefix) {
				continue
			}
			if secs, err := strconv.Atoi(part[len(prefix):]); err == nil && secs > 0 {
				if secs > int(maxCacheTTL/time.Second) {
					ttl = maxCacheTTL
					continue
				}
				ttl = time.Duration(secs) * time.Second
			}
		}
	}
	return ttl
}
