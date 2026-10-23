// SPDX-License-Identifier: MIT

package promscrape

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
)

type authLookupFunc func(context.Context, string) (string, error)

func (f authLookupFunc) ScrapeAuth(ctx context.Context, ref string) (string, error) {
	return f(ctx, ref)
}

func TestAuthFallbackRechecksExpiryAndRevocationAfterFetch(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		name := "expires during fetch"
		if revoke {
			name = "revoked by concurrent fetch"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Unix(1000, 0)
			want := errors.New("metadata service unavailable")
			s := New(Config{})
			s.authNow = func() time.Time { return now }
			s.authCache["ns/creds/token"] = authCacheEntry{
				token: "old-token", fetched: now.Add(-authStaleGrace + time.Second), used: now,
			}
			s.cfg.Auth = authLookupFunc(func(context.Context, string) (string, error) {
				if revoke {
					s.authMu.Lock()
					delete(s.authCache, "ns/creds/token")
					s.authMu.Unlock()
				} else {
					now = now.Add(2 * time.Second)
				}
				return "", want
			})
			if tok, err := s.authToken(t.Context(), "ns/creds/token"); tok != "" || !errors.Is(err, want) {
				t.Fatalf("authToken: token=%q, error=%v; want the lookup error", tok, err)
			}
			if len(s.authCache) != 0 {
				t.Fatal("expired or revoked credential is still cached")
			}
		})
	}
}

func TestAuthRefusalDoesNotDiscardConcurrentRefresh(t *testing.T) {
	now := time.Unix(1000, 0)
	s := New(Config{})
	s.authNow = func() time.Time { return now }
	s.authCache["ns/creds/token"] = authCacheEntry{
		token: "old-token", fetched: now.Add(-2 * authCacheTTL), used: now,
	}
	s.cfg.Auth = authLookupFunc(func(context.Context, string) (string, error) {
		s.authMu.Lock()
		s.authCache["ns/creds/token"] = authCacheEntry{token: "rotated", fetched: now, used: now}
		s.authMu.Unlock()
		return "", &metaclient.StatusError{Code: http.StatusForbidden}
	})
	if _, err := s.authToken(t.Context(), "ns/creds/token"); err == nil {
		t.Fatal("the refused request must still fail")
	}
	if tok, err := s.authToken(t.Context(), "ns/creds/token"); err != nil || tok != "rotated" {
		t.Fatalf("concurrently refreshed credential was discarded: token=%q, error=%v", tok, err)
	}
}
