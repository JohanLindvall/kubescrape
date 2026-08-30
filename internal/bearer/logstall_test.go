package bearer

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// gateWriter is a log destination that BLOCKS on the one record naming needle
// and passes everything else through. It is the container's stderr pipe with
// nothing draining it: a full log disk, a stalled collector, a 64 KiB pipe
// whose reader died. slog writes are ordinary blocking io.Writer writes, so
// this is what a log write costs on an unhealthy node — which is exactly the
// node whose Secret projection is also failing to re-read.
type gateWriter struct {
	needle  string
	started chan struct{}
	release chan struct{}
	once    sync.Once

	mu  sync.Mutex
	buf bytes.Buffer
}

func newGate(needle string) *gateWriter {
	return &gateWriter{needle: needle, started: make(chan struct{}), release: make(chan struct{})}
}

func (w *gateWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), w.needle) {
		w.once.Do(func() { close(w.started) })
		<-w.release
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *gateWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// Tokens() drops r.mu across the file read precisely so a wedged Secret mount
// cannot serialise every authenticated request. Reporting the OUTCOME of that
// read under the lock re-opens the same hole through a different blocking
// syscall: r.log.Warn/Info is a handler call plus a write to stderr, and on a
// node whose log writer has stalled that write does not return. Every
// concurrent auth check — every agent's /v1/scrape-auth fetch, every sibling
// shard's internal span push on the trace tier, where the spans in flight are
// terminal and in memory — would park behind it.
//
// All three records the re-read can produce are covered, because closing one
// and leaving its siblings is how this defect got re-introduced.
//
// Reverse-patch check: moving any of the three log calls back inside the
// `if` block (i.e. before r.mu.Unlock()) makes the matching subtest hang to
// its deadline and fail.
func TestTokensDoesNotHoldTheLockAcrossALogWrite(t *testing.T) {
	cases := []struct {
		name   string
		needle string
		// arm puts the Rotating in the state whose NEXT Tokens() emits the
		// gated record, and returns nothing: the test calls Tokens() itself.
		arm func(t *testing.T, r *Rotating, c *clock, path string)
	}{
		{
			// The rotation Info: the file changed under us.
			name:   "rotation",
			needle: "rotated",
			arm: func(t *testing.T, r *Rotating, c *clock, path string) {
				if err := os.WriteFile(path, []byte("second"), 0o600); err != nil {
					t.Fatal(err)
				}
				c.advance(2 * DefaultRefreshInterval)
			},
		},
		{
			// The read-failure Warn — the branch that fires when the mount is
			// ALREADY unhealthy, which is when this stall would land.
			name:   "read failure",
			needle: "failed",
			arm: func(t *testing.T, r *Rotating, c *clock, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				c.advance(2 * DefaultRefreshInterval)
			},
		},
		{
			// The recovery Info, reached only after a failure.
			name:   "recovery",
			needle: "succeeded again",
			arm: func(t *testing.T, r *Rotating, c *clock, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				c.advance(2 * DefaultRefreshInterval)
				_ = r.Tokens() // fails; its Warn is not gated
				// Restore the SAME contents, so recovery is the only record.
				if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
					t.Fatal(err)
				}
				c.advance(2 * DefaultReadInterval) // a failed read backs off
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
				t.Fatal(err)
			}
			gate := newGate(tc.needle)
			log := slog.New(slog.NewTextHandler(gate, &slog.HandlerOptions{Level: slog.LevelDebug}))
			c := newClock()
			r, err := NewRotating(path, log, WithClock(c.now))
			if err != nil {
				t.Fatal(err)
			}
			tc.arm(t, r, c, path)

			stuck := make(chan struct{})
			go func() {
				defer close(stuck)
				_ = r.Tokens()
			}()
			select {
			case <-gate.started:
			case <-time.After(5 * time.Second):
				t.Fatal("the gated log record was never written; the arm did not reach it")
			}

			// The attack: with the log write parked, any other caller's auth
			// check must still be answered.
			answered := make(chan []string, 1)
			go func() { answered <- r.Tokens() }()
			select {
			case got := <-answered:
				if len(got) == 0 || got[0] == "" {
					t.Errorf("Tokens() = %v while a log write is stalled; want the accept set", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Tokens() blocked behind a stalled log write: r.mu is held across the slog call, " +
					"so every authenticated request serialises behind the node's log writer")
			}

			// Releasing the writer must still yield the record: the fix moves
			// the reporting out of the critical section, it does not drop it.
			close(gate.release)
			select {
			case <-stuck:
			case <-time.After(5 * time.Second):
				t.Fatal("the logging Tokens() never returned after the writer was released")
			}
			if out := gate.String(); !strings.Contains(out, tc.needle) {
				t.Errorf("the %s record never reached the log; got:\n%s", tc.name, out)
			}
		})
	}
}
