// SPDX-License-Identifier: MIT

package metaclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestResponseDisablingCacheRemovesOldEntry(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotModified} {
		for _, policy := range []string{"no-store", `no-cache="ETag"`, "max-age=0", ""} {
			t.Run(http.StatusText(status)+"/"+policy, func(t *testing.T) {
				var hits atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch hits.Add(1) {
					case 1:
						w.Header().Set("Cache-Control", "max-age=1")
						w.Header().Set("ETag", `"old"`)
					case 2:
						if got := r.Header.Get("If-None-Match"); got != `"old"` {
							t.Errorf("revalidation ETag = %q", got)
						}
						w.Header().Set("Cache-Control", policy)
						w.WriteHeader(status)
						if status == http.StatusNotModified {
							return
						}
					default:
						if got := r.Header.Get("If-None-Match"); got != "" {
							t.Errorf("uncacheable response left the old validator %q", got)
						}
					}
					_, _ = w.Write([]byte(`{"name":"web"}`))
				}))
				t.Cleanup(srv.Close)
				c := New(Config{Base: srv.URL, Timeout: time.Second})
				now := time.Now()
				c.now = func() time.Time { return now }
				for range 3 {
					if _, err := c.PodByUID(t.Context(), "u1"); err != nil {
						t.Fatal(err)
					}
					now = now.Add(2 * time.Second)
				}
				if len(c.cache) != 0 {
					t.Fatal("uncacheable response remains stored")
				}
			})
		}
	}
}

func TestUnsolicitedNotModifiedWithUnvalidatedCache(t *testing.T) {
	s := newSrv(t)
	s.maxAge = "1" // Cacheable, but no validator was supplied.
	c := New(Config{Base: s.URL, Timeout: time.Second})
	now := time.Now()
	c.now = func() time.Time { return now }
	if _, err := c.PodByName(context.Background(), "ns", "p"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	s.mu.Lock()
	s.code = http.StatusNotModified
	s.mu.Unlock()
	_, err := c.PodByName(t.Context(), "ns", "p")
	se, ok := errors.AsType[*StatusError](err)
	if !ok || se.Code != http.StatusNotModified {
		t.Fatalf("unsolicited 304: %v, want a status error", err)
	}
}

func TestResponseCannotChangeConcurrentCachePolicy(t *testing.T) {
	for _, tc := range []struct {
		status int
		policy string
	}{
		{http.StatusNotModified, "max-age=60"},
		{http.StatusNotModified, "no-store"},
		{http.StatusOK, "no-store"},
	} {
		t.Run(http.StatusText(tc.status)+"/"+tc.policy, func(t *testing.T) {
			now := time.Now()
			const key = "http://metadata.test/v1/pod-uids/u1"
			newExpiry := now.Add(time.Hour)
			c := New(Config{Base: "http://metadata.test"})
			c.now = func() time.Time { return now }
			c.cache[key] = cacheEntry{
				decoded: &kubemeta.Pod{Name: "web"}, etag: `"same-body"`, expires: now, used: now,
			}
			c.http.Transport = responseTransport(func(*http.Request) (*http.Response, error) {
				// Another fetch has completed while this request was in flight.
				// The body and ETag agree, but its cache lifetime is different.
				c.mu.Lock()
				c.cache[key] = cacheEntry{
					decoded: &kubemeta.Pod{Name: "web"}, etag: `"same-body"`, expires: newExpiry, used: now,
				}
				c.mu.Unlock()
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Cache-Control": {tc.policy}},
					Body: io.NopCloser(strings.NewReader(`{"name":"web"}`))}, nil
			})
			if _, err := c.PodByUID(t.Context(), "u1"); err != nil {
				t.Fatal(err)
			}
			if got := c.cache[key].expires; !got.Equal(newExpiry) {
				t.Errorf("concurrent entry expires at %v, want %v", got, newExpiry)
			}
		})
	}
}
