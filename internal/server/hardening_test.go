package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/JohanLindvall/kubescrape/internal/services"
	"github.com/JohanLindvall/kubescrape/internal/store"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// newAPI builds a Server (not the httptest wrapper) so tests can exercise
// HTTPServer's timeout wiring on a real listener.
func newAPI(st *store.Store, maxWait time.Duration) *Server {
	return New(Config{
		Store:    st,
		Services: services.NewIndex(),
		Resolver: stubResolver{},
		MaxWait:  maxWait,
		Ready:    closedChan(),
	})
}

// listenAndServe runs srv on an ephemeral port and returns its base URL.
func listenAndServe(t *testing.T, srv *http.Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return "http://" + ln.Addr().String()
}

// The production timeout profile: header trickling and idle keep-alives are
// bounded, and the request-scoped timeouts always exceed the wait budget so
// the container-wait endpoint cannot be cut off by them.
func TestHTTPServerTimeoutProfile(t *testing.T) {
	maxWait := 7 * time.Second
	srv := newAPI(store.New(time.Minute), maxWait).HTTPServer(":0")
	if srv.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", srv.ReadHeaderTimeout)
	}
	if srv.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want 120s", srv.IdleTimeout)
	}
	if srv.ReadTimeout <= maxWait {
		t.Errorf("ReadTimeout = %v must exceed MaxWait %v (background read deadline cancels in-flight waits)", srv.ReadTimeout, maxWait)
	}
	if srv.WriteTimeout <= maxWait {
		t.Errorf("WriteTimeout = %v must exceed MaxWait %v (wait responses are written after the hold)", srv.WriteTimeout, maxWait)
	}
}

// Slowloris: clients trickling a request forever must be disconnected by the
// header/read deadlines and their goroutines reclaimed, instead of each
// pinning a connection + goroutine indefinitely.
func TestSlowClientsAreDropped(t *testing.T) {
	srv := newAPI(store.New(time.Minute), 50*time.Millisecond).HTTPServer(":0")
	// The production values (10s/35s+) are correct but too slow for a unit
	// test; shrink them while keeping the exact production wiring.
	srv.ReadHeaderTimeout = 150 * time.Millisecond
	srv.ReadTimeout = 300 * time.Millisecond
	srv.WriteTimeout = 300 * time.Millisecond
	base := listenAndServe(t, srv)
	addr := strings.TrimPrefix(base, "http://")

	before := runtime.NumGoroutine()

	const n = 20
	var closedWithin atomic.Int64
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			// Trickle an eternal request line, one byte at a time.
			start := time.Now()
			for _, b := range []byte("GET /v1/containers/abc HTTP/1.1\r\nHost: x\r\nX-Slow: ") {
				if _, err := conn.Write([]byte{b}); err != nil {
					break // server hung up: the defense worked
				}
				time.Sleep(20 * time.Millisecond)
			}
			// However far we got, the server must close the connection.
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _ = io.Copy(io.Discard, conn)
			if time.Since(start) < 4*time.Second {
				closedWithin.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := closedWithin.Load(); got != n {
		t.Fatalf("only %d/%d slow connections were dropped promptly", got, n)
	}

	// Goroutines return to (near) baseline once the connections are gone.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+3 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutines leaked: before=%d after=%d", before, runtime.NumGoroutine())
}

// A container-wait request held for (nearly) the full budget must survive the
// server timeouts and still resolve when the metadata arrives mid-wait.
func TestWaitSurvivesServerTimeouts(t *testing.T) {
	st := store.New(time.Minute)
	maxWait := 600 * time.Millisecond
	srv := newAPI(st, maxWait).HTTPServer(":0")
	// Tighten the slack: keep timeouts > maxWait (the invariant) but small
	// enough that a violation would fail the test quickly.
	srv.ReadTimeout = maxWait + 400*time.Millisecond
	srv.WriteTimeout = maxWait + 400*time.Millisecond
	base := listenAndServe(t, srv)

	go func() {
		time.Sleep(400 * time.Millisecond) // most of the budget
		addPod(st)
	}()
	start := time.Now()
	resp, err := http.Get(base + "/v1/containers/cafe01")
	if err != nil {
		t.Fatalf("wait request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d after %v, want 200", resp.StatusCode, time.Since(start))
	}
	var got kubemeta.ContainerMetadata
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Pod.Name != "web-abc-xyz" {
		t.Fatalf("pod %q", got.Pod.Name)
	}
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("resolved in %v; the request never actually waited", d)
	}
}

// Hostile container IDs: kilobytes-long path segments are rejected up front
// (400), never registered as waiter keys, and never allocate wait budget.
func TestContainerIDTooLong(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())

	start := time.Now()
	huge := strings.Repeat("a", 64<<10)
	resp, err := http.Get(srv.URL + "/v1/containers/" + huge)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("oversized ID burned %v of wait budget", d)
	}
}

// Waiter overflow surfaces as a retryable 503 (with Retry-After), never 404.
func TestContainerWaiterOverflow503(t *testing.T) {
	st := store.New(time.Minute)
	st.SetMaxWaiters(2)
	srv := httptest.NewServer(newAPI(st, 10*time.Second).Handler())
	defer srv.Close()

	// Fill the cap with two lookups parked directly on the store (an HTTP
	// filler would race the polls below for slots); released at test end.
	pctx, pcancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = st.GetContainer(pctx, fmt.Sprintf("park%d", i))
		}()
	}
	defer func() {
		pcancel()
		wg.Wait()
	}()

	// Poll with short-wait lookups until both fillers are parked: at the cap
	// the lookup is shed immediately as 503 + Retry-After.
	deadline := time.Now().Add(5 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		resp, err := http.Get(srv.URL + "/v1/containers/overflow1?wait=50ms")
		if err != nil {
			t.Fatal(err)
		}
		last = resp.StatusCode
		retryAfter := resp.Header.Get("Retry-After")
		_ = resp.Body.Close()
		if last == http.StatusServiceUnavailable {
			if retryAfter == "" {
				t.Error("503 without Retry-After")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("never saw 503 at waiter cap (last status %d)", last)
}

// Hostile paths must produce clean 4xx/404s, never hangs or panics: invalid
// UTF-8 in pod names, NUL bytes, huge query strings, and a chunked body
// trickled on a GET (the handlers never read bodies; net/http's post-handler
// drain is bounded by ReadTimeout).
func TestHostilePaths(t *testing.T) {
	st := store.New(time.Minute)
	addPod(st)
	srv := testServer(t, st, closedChan())

	cases := []struct {
		path string
		want []int
	}{
		{"/v1/pods/def%FFault/na%FEme", []int{http.StatusNotFound, http.StatusBadRequest}},
		{"/v1/pods/default/" + strings.Repeat("%00", 100), []int{http.StatusNotFound, http.StatusBadRequest}},
		{"/v1/pod-ips/" + strings.Repeat("9", 10000), []int{http.StatusNotFound}},
		{"/v1/nodes/" + strings.Repeat("x", 10000) + "/targets", []int{http.StatusOK, http.StatusNotFound}},
		{"/v1/containers/cafe01?wait=" + strings.Repeat("9", 5000), []int{http.StatusBadRequest, http.StatusOK}},
		{"/v1/containers/..%2F..%2Fetc%2Fpasswd", []int{http.StatusNotFound, http.StatusBadRequest, http.StatusMovedPermanently}},
	}
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for _, tc := range cases {
		resp, err := client.Get(srv.URL + tc.path)
		if err != nil {
			t.Errorf("GET %.60s...: %v", tc.path, err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		okStatus := false
		for _, w := range tc.want {
			if resp.StatusCode == w {
				okStatus = true
			}
		}
		if !okStatus {
			t.Errorf("GET %.60s...: status %d, want one of %v", tc.path, resp.StatusCode, tc.want)
		}
	}

}

// An unterminated chunked body on a GET pins the connection: the handler runs
// and returns, but net/http drains the request body before flushing the
// response, so without ReadTimeout the connection (and its goroutine) hangs
// forever waiting for the next chunk — this test wedged indefinitely before
// ReadTimeout was set. With it, the drain hits the whole-request read deadline
// and the connection is torn down.
func TestChunkedGETBodyDrainIsBounded(t *testing.T) {
	srv := newAPI(store.New(time.Minute), 50*time.Millisecond).HTTPServer(":0")
	srv.ReadTimeout = 500 * time.Millisecond // production shape, test speed
	srv.WriteTimeout = 500 * time.Millisecond
	base := listenAndServe(t, srv)

	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	start := time.Now()
	// A complete chunk but no terminating 0-chunk: the body never ends.
	_, _ = io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	// Whatever the server sends (a late response or nothing), the connection
	// must be CLOSED once ReadTimeout fires — never pinned indefinitely.
	_, _ = io.Copy(io.Discard, conn)
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("connection stayed pinned %v; body drain not bounded by ReadTimeout", d)
	}
}

// TestServerConcurrentLoad hammers the four read endpoints from 32 goroutines
// while pods churn underneath, through the real HTTP stack. Correctness under
// load: every 200 container response names the container's own pod, targets
// responses always parse, and only expected statuses appear. Run under -race.
func TestServerConcurrentLoad(t *testing.T) {
	st := store.New(time.Minute)
	srv := testServer(t, st, closedChan())
	duration := 2 * time.Second
	if testing.Short() {
		duration = 500 * time.Millisecond
	}

	const pods = 16
	podName := func(p int) string { return fmt.Sprintf("load-pod-%d", p) }
	contID := func(p int) string { return fmt.Sprintf("dead%04d", p) }
	upsert := func(p, rv int) {
		st.UpsertPod(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName(p), Namespace: "default",
				UID: types.UID(fmt.Sprintf("load-uid-%d", p)), ResourceVersion: fmt.Sprint(rv),
				Annotations: map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "8080"},
			},
			Spec: corev1.PodSpec{NodeName: "load-node", Containers: []corev1.Container{{Name: "app", Image: "img"}}},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning, PodIP: fmt.Sprintf("10.9.0.%d", p+1),
				ContainerStatuses: []corev1.ContainerStatus{{
					Name: "app", ContainerID: "containerd://" + contID(p),
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				}},
			},
		})
	}
	for p := range pods {
		upsert(p, 1)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writer: churn upserts/deletes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rv := 2
		for {
			for p := range pods {
				select {
				case <-stop:
					return
				default:
				}
				rv++
				if rv%5 == 0 {
					st.DeletePod(types.UID(fmt.Sprintf("load-uid-%d", p)))
				} else {
					upsert(p, rv)
				}
			}
		}
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	var requests atomic.Int64
	errCh := make(chan string, 1)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	for r := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := r
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				p := i % pods
				switch i % 4 {
				case 0:
					resp, err := client.Get(fmt.Sprintf("%s/v1/containers/%s?wait=0s", srv.URL, contID(p)))
					if err != nil {
						fail("containers: %v", err)
						return
					}
					if resp.StatusCode == http.StatusOK {
						var got kubemeta.ContainerMetadata
						if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
							fail("containers decode: %v", err)
						} else if got.Pod.Name != podName(p) {
							fail("container %s resolved to pod %q, want %q", contID(p), got.Pod.Name, podName(p))
						}
					} else if resp.StatusCode != http.StatusNotFound {
						fail("containers status %d", resp.StatusCode)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				case 1:
					resp, err := client.Get(fmt.Sprintf("%s/v1/pod-ips/10.9.0.%d", srv.URL, p+1))
					if err != nil {
						fail("pod-ips: %v", err)
						return
					}
					if resp.StatusCode == http.StatusOK {
						var got kubemeta.Pod
						if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
							fail("pod-ips decode: %v", err)
						} else if got.DeletedAt != nil {
							fail("pod-ips returned deleted pod %s", got.Name)
						}
					} else if resp.StatusCode != http.StatusNotFound {
						fail("pod-ips status %d", resp.StatusCode)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				case 2:
					resp, err := client.Get(srv.URL + "/v1/nodes/load-node/targets")
					if err != nil {
						fail("targets: %v", err)
						return
					}
					var got struct {
						Targets []kubemeta.ScrapeTarget `json:"targets"`
					}
					if resp.StatusCode != http.StatusOK {
						fail("targets status %d", resp.StatusCode)
					} else if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
						fail("targets decode: %v", err)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				case 3:
					resp, err := client.Get(fmt.Sprintf("%s/v1/pods/default/%s", srv.URL, podName(p)))
					if err != nil {
						fail("pods: %v", err)
						return
					}
					if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
						fail("pods status %d", resp.StatusCode)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				requests.Add(1)
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	select {
	case msg := <-errCh:
		t.Fatal(msg)
	default:
	}
	t.Logf("HTTP load: %d requests in %v (%.0f req/s)", requests.Load(), duration, float64(requests.Load())/duration.Seconds())
	if requests.Load() == 0 {
		t.Fatal("load test did no work")
	}
}
