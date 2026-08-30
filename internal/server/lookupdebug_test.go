package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
)

// The container endpoint's transitions — a lookup that PARKED, one the waiter
// cap refused, and the readiness park's outcome — were counters only. A counter
// says how many and never WHICH, so during "the agent's first poll returns
// nothing" there was no per-request line anywhere in this package to read: it
// had zero Debug calls.
//
// What these tests also pin is the other half of the rule: the route is polled
// by every agent for every log file, so the warm path must stay silent, and
// nothing may be evaluated at all below Debug.

// logBuf is written from handler goroutines while the test reads it.
type logBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// loggedAPI is newAPI with the log captured at a chosen level and an
// overridable readiness channel.
func loggedAPI(st *store.Store, maxWait time.Duration, level slog.Level, ready <-chan struct{}) (*Server, *logBuf) {
	buf := &logBuf{}
	return New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  maxWait,
		Ready:    ready,
		Log:      slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})),
	}), buf
}

func getContainer(t *testing.T, srv *Server, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// A lookup that spends its whole budget parked and then 404s is the shape the
// timeout counter reports in aggregate; the Debug line is what names the id.
func TestBlockedContainerLookupIsLoggedAtDebug(t *testing.T) {
	srv, buf := loggedAPI(store.New(time.Minute), time.Second, slog.LevelDebug, closedChan())
	if code := getContainer(t, srv, "/v1/containers/neverappears?wait=40ms"); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
	line := buf.String()
	if !strings.Contains(line, "container lookup blocked and then woke") {
		t.Fatalf("no blocked-lookup line in %q", line)
	}
	for _, want := range []string{"id=neverappears", "found=false", "waited="} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q is missing %q", line, want)
		}
	}
}

// The same request at Info emits nothing: this route carries the highest
// request rate in the process, and slog evaluates its arguments eagerly, so the
// clock read and the line both have to sit behind the level check.
func TestBlockedContainerLookupIsSilentAtInfo(t *testing.T) {
	srv, buf := loggedAPI(store.New(time.Minute), time.Second, slog.LevelInfo, closedChan())
	if code := getContainer(t, srv, "/v1/containers/neverappears?wait=40ms"); code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
	// The throttled timeout WARN is expected here and is not per-request; what
	// must not appear is the per-request Debug seam.
	if s := buf.String(); strings.Contains(s, "level=DEBUG") {
		t.Fatalf("Info level emitted a Debug line: %q", s)
	}
}

// A HIT must be silent even at Debug: it is the warm path, once per log file
// per agent per cycle.
func TestWarmContainerLookupIsSilentAtDebug(t *testing.T) {
	st := store.New(time.Minute)
	st.UpsertPod(podWithContainer("ns", "p1", "pod-uid", "containerd://abc123"))
	srv, buf := loggedAPI(st, time.Second, slog.LevelDebug, closedChan())
	if code := getContainer(t, srv, "/v1/containers/containerd:%2F%2Fabc123?wait=1s"); code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if s := buf.String(); s != "" {
		t.Fatalf("a warm lookup logged: %q", s)
	}
}

// The waiter cap's refusal is a 503 the agent retries; the counter it moves is
// shared with the readiness park, so the line is the only thing that says which
// id was refused.
func TestShedContainerLookupIsLoggedAtDebug(t *testing.T) {
	st := store.New(time.Minute)
	st.SetMaxWaiters(0)
	srv, buf := loggedAPI(st, time.Second, slog.LevelDebug, closedChan())
	if code := getContainer(t, srv, "/v1/containers/neverappears?wait=1s"); code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", code)
	}
	line := buf.String()
	if !strings.Contains(line, "container lookup refused before it could wait") ||
		!strings.Contains(line, "id=neverappears") {
		t.Fatalf("shed line missing or unnamed in %q", line)
	}
}

// The SECOND parking spot. Its outcome is what answers "why did the agent's
// first poll return nothing?" — the caches were still filling and the budget
// expired, which no status code distinguishes from a plain miss.
func TestReadinessParkLogsExpiredOutcome(t *testing.T) {
	unready := make(chan struct{})
	srv, buf := loggedAPI(store.New(time.Minute), time.Second, slog.LevelDebug, unready)
	if code := getContainer(t, srv, "/v1/containers/neverappears?wait=40ms"); code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", code)
	}
	line := buf.String()
	if !strings.Contains(line, "container lookup left the readiness park") ||
		!strings.Contains(line, "outcome=expired") {
		t.Fatalf("readiness-park line missing or mislabelled in %q", line)
	}
}

// A park released by the DRAIN reports draining, not expired: the two take
// different remedies (wait vs. the request moved to another replica).
func TestReadinessParkLogsDrainingOutcome(t *testing.T) {
	st := store.New(time.Minute)
	unready := make(chan struct{})
	srv, buf := loggedAPI(st, 30*time.Second, slog.LevelDebug, unready)
	hsrv := httptest.NewServer(srv.Handler())
	defer hsrv.Close()

	done := make(chan int, 1)
	go func() {
		resp, err := http.Get(hsrv.URL + "/v1/containers/neverappears?wait=30s")
		if err != nil {
			done <- 0
			return
		}
		defer func() { _ = resp.Body.Close() }()
		done <- resp.StatusCode
	}()
	deadline := time.Now().Add(5 * time.Second)
	for st.BlockedLookups() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("lookup never reached the readiness park")
		}
		time.Sleep(time.Millisecond)
	}
	srv.Drain()

	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parked lookup was not answered")
	}
	if line := buf.String(); !strings.Contains(line, "outcome=draining") {
		t.Fatalf("drained park reported the wrong outcome: %q", line)
	}
}

// The readiness park draws on the SAME cap as the store's waiters, so its
// refusal moves a counter that cannot say which spot bound. Only this line can.
func TestReadinessParkShedIsLoggedAtDebug(t *testing.T) {
	st := store.New(time.Minute)
	st.SetMaxWaiters(0)
	unready := make(chan struct{})
	srv, buf := loggedAPI(st, time.Second, slog.LevelDebug, unready)
	if code := getContainer(t, srv, "/v1/containers/neverappears?wait=1s"); code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", code)
	}
	if line := buf.String(); !strings.Contains(line, "shed while waiting for the initial informer sync") {
		t.Fatalf("readiness-park shed was not logged: %q", line)
	}
}

// podWithContainer builds a running pod carrying one running container id.
func podWithContainer(ns, name, uid, containerID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Spec:       corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{{Name: "app"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", ContainerID: containerID,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}
