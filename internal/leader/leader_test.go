package leader

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The fake clientset does not enforce the optimistic-concurrency conflicts
// that mutual exclusion actually rests on, so these cover the WIRING — the
// callbacks, the context contract, and the re-entry loop. The exclusion
// guarantee itself belongs to client-go's own suite.

func testConfig(work func(context.Context)) Config {
	return Config{
		Client:        fake.NewSimpleClientset(),
		Namespace:     "monitoring",
		Name:          "kubescrape-cluster-leader",
		Identity:      "pod-a",
		LeaseDuration: 300 * time.Millisecond,
		RenewDeadline: 200 * time.Millisecond,
		RetryPeriod:   40 * time.Millisecond,
		OnStarted:     work,
	}
}

// The happy path: the work runs while leading, its context is cancelled on
// shutdown, and Run returns only after that.
func TestRunsWorkAndStopsOnCancel(t *testing.T) {
	started := make(chan struct{})
	var stopped atomic.Bool
	cfg := testConfig(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		stopped.Store(true)
	})
	var leading []bool
	var mu sync.Mutex
	cfg.OnLeading = func(v bool) { mu.Lock(); leading = append(leading, v); mu.Unlock() }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("never acquired the lease")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if !stopped.Load() {
		t.Fatal("the work's context was not cancelled")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(leading) == 0 || !leading[0] {
		t.Fatalf("leadership transitions = %v, want a leading=true first", leading)
	}
}

// Work that ignores its context must not wedge the process silently: Run
// gives up after the renew deadline and reports it, rather than re-entering
// the election with the previous worker still alive.
func TestWorkThatIgnoresContextIsReported(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	cfg := testConfig(func(context.Context) { <-release })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	time.Sleep(300 * time.Millisecond) // let it acquire
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not stop") {
			t.Fatalf("err = %v, want a did-not-stop error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run neither returned nor reported the stuck work")
	}
}

// A replica that never leads must not report a LOST lease on shutdown —
// client-go calls OnStoppedLeading even when the acquire never succeeded,
// which would otherwise put a lie in every non-leader's shutdown log.
func TestNeverLedReportsNoLoss(t *testing.T) {
	client := fake.NewSimpleClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "kubescrape-cluster-leader", Namespace: "monitoring"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("pod-b"),
			LeaseDurationSeconds: ptr(int32(3600)),
			RenewTime:            &metav1.MicroTime{Time: time.Now()},
			AcquireTime:          &metav1.MicroTime{Time: time.Now()},
		},
	})
	var transitions atomic.Int32
	var ranWork atomic.Bool
	cfg := Config{
		Client: client, Namespace: "monitoring", Name: "kubescrape-cluster-leader",
		Identity: "pod-a",
		// Long enough that the held lease never looks expired during the test.
		LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 100 * time.Millisecond,
		OnStarted: func(ctx context.Context) { ranWork.Store(true); <-ctx.Done() },
		OnLeading: func(bool) { transitions.Add(1) },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	time.Sleep(400 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	if ranWork.Load() {
		t.Fatal("leader-only work ran without the lease")
	}
	if got := transitions.Load(); got != 0 {
		t.Fatalf("a replica that never led reported %d leadership transitions, want 0", got)
	}
}

func ptr[T any](v T) *T { return &v }

// Configuration errors surface instead of panicking (which RunOrDie would do).
func TestConfigValidation(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no client":    {Namespace: "n", Name: "l", OnStarted: func(context.Context) {}},
		"no namespace": {Client: fake.NewSimpleClientset(), Name: "l", OnStarted: func(context.Context) {}},
		"no name":      {Client: fake.NewSimpleClientset(), Namespace: "n", OnStarted: func(context.Context) {}},
		"no work":      {Client: fake.NewSimpleClientset(), Namespace: "n", Name: "l"},
	} {
		if err := Run(context.Background(), cfg); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	// Durations that violate LeaseDuration > RenewDeadline > RetryPeriod*1.2
	// are rejected by client-go, and we surface that rather than panicking.
	bad := testConfig(func(context.Context) {})
	bad.LeaseDuration, bad.RenewDeadline = time.Second, 2*time.Second
	if err := Run(context.Background(), bad); err == nil {
		t.Error("inverted durations must be rejected")
	}
}

func TestNamespaceFromEnv(t *testing.T) {
	t.Setenv("POD_NAMESPACE", " monitoring ")
	if got := Namespace(); got != "monitoring" {
		t.Fatalf("Namespace() = %q, want the trimmed env value", got)
	}
}
