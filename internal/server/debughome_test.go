package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The homepage serves UNREADY (that is when someone reaches for it), carries
// the explain form, and the root redirects to it.
func TestDebugHomepage(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, make(chan struct{})) // never ready

	resp, err := http.Get(srv.URL + "/debug")
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 1<<16)
	n, _ := resp.Body.Read(body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body[:n]), "/v1/explain/") {
		t.Fatalf("homepage = %d %.80q", resp.StatusCode, body[:n])
	}

	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	rr, err := noRedirect.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = rr.Body.Close()
	if rr.StatusCode != http.StatusTemporaryRedirect || rr.Header.Get("Location") != "/debug" {
		t.Fatalf("root = %d %q, want redirect to /debug", rr.StatusCode, rr.Header.Get("Location"))
	}
}
