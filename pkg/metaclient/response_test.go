// SPDX-License-Identifier: MIT

package metaclient

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type responseTransport func(*http.Request) (*http.Response, error)

func (f responseTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type spaceReader struct{}

func (spaceReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	return len(p), nil
}

type delayedBody struct {
	io.Reader
	advance func()
}

func (r *delayedBody) Read(p []byte) (int, error) {
	if r.advance != nil {
		r.advance()
		r.advance = nil
	}
	return r.Reader.Read(p)
}

func TestCacheLifetimeIncludesTimeSpentFetching(t *testing.T) {
	for _, inBody := range []bool{false, true} {
		name := "waiting for headers"
		if inBody {
			name = "reading the body"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(1000, 0)
			hits := 0
			c := New(Config{
				Base: "http://metadata.test",
				Transport: responseTransport(func(r *http.Request) (*http.Response, error) {
					hits++
					body := &delayedBody{Reader: strings.NewReader(`{"name":"web"}`)}
					if hits == 1 {
						advance := func() { now = now.Add(20 * time.Second) }
						if inBody {
							body.advance = advance
						} else {
							advance()
						}
					} else if r.Header.Get("If-None-Match") != `"v1"` {
						t.Error("the expired response should retain its validator")
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(body), Header: http.Header{
						"Cache-Control": {"max-age=10"}, "Etag": {`"v1"`},
					}}, nil
				}),
			})
			c.now = func() time.Time { return now }
			for range 3 {
				if _, err := c.PodByUID(t.Context(), "u1"); err != nil {
					t.Fatal(err)
				}
			}
			if hits != 2 {
				t.Errorf("requests = %d, want 2: a slow response cannot restart its max-age at completion", hits)
			}
		})
	}
}

func TestMetadataResponseSizeBoundary(t *testing.T) {
	for _, extra := range []string{"", "x"} {
		name := "exact limit"
		if extra != "" {
			name = "valid prefix with oversized suffix"
		}
		t.Run(name, func(t *testing.T) {
			var outcome string
			c := New(Config{
				Base:    "http://metadata.test",
				Observe: func(o string) { outcome = o },
				Transport: responseTransport(func(*http.Request) (*http.Response, error) {
					const body = `{"name":"web"}`
					r := io.MultiReader(strings.NewReader(body),
						io.LimitReader(spaceReader{}, maxResponseBytes-int64(len(body))), strings.NewReader(extra))
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Cache-Control": {"max-age=60"}}, Body: io.NopCloser(r)}, nil
				}),
			})
			pod, err := c.PodByUID(t.Context(), "u1")
			if extra == "" {
				if err != nil || pod.Name != "web" || outcome != OutcomeOK {
					t.Fatalf("exact limit: pod=%v error=%v outcome=%s", pod, err, outcome)
				}
				return
			}
			if _, ok := errors.AsType[*DecodeError](err); !ok || outcome != OutcomeError || len(c.cache) != 0 {
				t.Fatalf("oversized response: error=%v outcome=%s cached=%d", err, outcome, len(c.cache))
			}
		})
	}
}

func TestMetadataClientRefusesRedirects(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var forwarded atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				forwarded.Add(1)
				_, _ = w.Write([]byte(`{"value":"secret"}`))
			}))
			t.Cleanup(destination.Close)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, destination.URL, status)
			}))
			t.Cleanup(source.Close)
			c := New(Config{Base: source.URL, Timeout: time.Second, ScrapeAuthToken: func() string { return "credential" }})
			_, err := c.ScrapeAuth(t.Context(), "ns/name/key")
			se, ok := errors.AsType[*StatusError](err)
			if !ok || se.Code != status || forwarded.Load() != 0 {
				t.Fatalf("redirect: error=%v forwarded requests=%d", err, forwarded.Load())
			}
		})
	}
}
