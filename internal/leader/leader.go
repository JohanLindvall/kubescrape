// Package leader runs cluster-singleton work under a coordination.k8s.io
// Lease, so exactly one replica does it at a time.
//
// Losing the lease must NOT take the process down. The binary running this
// also serves other pipelines (and, in the metadata service, the API every
// agent blocks on), so a lease flap stops the leader-only work and nothing
// else — which is why this wraps client-go's elector in a re-entry loop
// instead of the usual os.Exit(1) in OnStoppedLeading.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Default timings, matching the ecosystem convention. The invariant
// leaderelection enforces is LeaseDuration > RenewDeadline > RetryPeriod*1.2;
// failover costs up to LeaseDuration after a hard kill and about RetryPeriod
// after a graceful stop (see ReleaseOnCancel below).
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRenewDeadline = 10 * time.Second
	DefaultRetryPeriod   = 2 * time.Second
)

// Config configures one election.
type Config struct {
	Client    kubernetes.Interface
	Namespace string // where the Lease lives (the pod's own namespace)
	Name      string // Lease name
	// Identity must be unique per replica; DefaultIdentity uses the pod name.
	Identity string

	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	// OnStarted does the leader-only work. It MUST return when ctx is done —
	// Run waits for it before competing again, so that a flap can never leave
	// two of them overlapping inside one process.
	OnStarted func(ctx context.Context)
	// OnLeading reports leadership transitions (a metric gauge, typically).
	OnLeading func(bool)

	Log *slog.Logger
}

// DefaultIdentity is the pod name, which is unique per replica of a
// Deployment. Note the deliberate consequence: a restarted pod with the SAME
// identity treats an existing Lease as its own and takes over without waiting
// out the duration — right for a Deployment (names carry a random suffix),
// but an overlap hazard under a StatefulSet's stable names.
func DefaultIdentity() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return fmt.Sprintf("unknown-%d", os.Getpid())
}

// Namespace resolves the pod's own namespace: $POD_NAMESPACE (downward API)
// first, then the ServiceAccount projection. Empty means neither was
// available, which the caller must treat as a configuration error — a Lease
// in the wrong namespace silently elects a separate leader per namespace.
func Namespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func (c *Config) defaults() error {
	if c.Client == nil {
		return errors.New("leader: no Kubernetes client")
	}
	if c.Namespace == "" {
		return errors.New("leader: no namespace (set POD_NAMESPACE via the downward API)")
	}
	if c.Name == "" {
		return errors.New("leader: no lease name")
	}
	if c.OnStarted == nil {
		return errors.New("leader: no work to run")
	}
	if c.Identity == "" {
		c.Identity = DefaultIdentity()
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.RenewDeadline <= 0 {
		c.RenewDeadline = DefaultRenewDeadline
	}
	if c.RetryPeriod <= 0 {
		c.RetryPeriod = DefaultRetryPeriod
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	if c.OnLeading == nil {
		c.OnLeading = func(bool) {}
	}
	return nil
}

// Run competes for the lease until ctx is done, running OnStarted whenever
// this replica holds it. It returns only on ctx cancellation or a
// configuration error.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.defaults(); err != nil {
		return err
	}
	cfg.Log.Info("competing for leadership", "lease", cfg.Name, "namespace", cfg.Namespace, "identity", cfg.Identity)
	for ctx.Err() == nil {
		if err := elect(ctx, &cfg); err != nil {
			return err
		}
		// Backoff before re-entering, so a persistently failing acquire does
		// not spin against the API server.
		select {
		case <-ctx.Done():
		case <-time.After(cfg.RetryPeriod):
		}
	}
	return nil
}

// elect runs ONE election round: acquire, work, and unwind after a loss. A
// fresh elector per round — re-running one that already observed a lease
// record is not a documented use of the API.
func elect(ctx context.Context, cfg *Config) error {
	var led atomic.Bool
	work := make(chan struct{})

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Name: cfg.Name, Namespace: cfg.Namespace},
			Client:     cfg.Client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: cfg.Identity},
		},
		// Hand the lease back on shutdown so the successor starts in
		// RetryPeriod instead of waiting out LeaseDuration. This only works if
		// Run is allowed to return before the process exits — keep the
		// goroutine calling it inside the process WaitGroup.
		ReleaseOnCancel: true,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(lctx context.Context) {
				defer close(work)
				led.Store(true)
				cfg.OnLeading(true)
				cfg.Log.Info("acquired leadership", "lease", cfg.Name)
				cfg.OnStarted(lctx)
			},
			OnStoppedLeading: func() {
				// Deferred inside Run BEFORE the acquire attempt, so this also
				// fires for a replica that never led (a plain shutdown while
				// waiting). Reporting a lost lease there would be a lie.
				if led.Load() {
					cfg.OnLeading(false)
					cfg.Log.Warn("lost leadership", "lease", cfg.Name)
				}
			},
			OnNewLeader: func(id string) {
				if id != cfg.Identity {
					cfg.Log.Info("another replica is leading", "lease", cfg.Name, "leader", id)
				}
			},
		},
	})
	if err != nil {
		return fmt.Errorf("leader: %w", err) // duration invariants, missing lock
	}

	le.Run(ctx) // returns on ctx done OR on a lost lease — never re-acquires

	if !led.Load() {
		return nil // never acquired; nothing to unwind
	}
	// OnStartedLeading runs in its own goroutine, so the work may still be
	// unwinding. Wait for it: re-entering the election with the previous
	// watcher still alive would put two of them in one process.
	select {
	case <-work:
		return nil
	case <-time.After(cfg.RenewDeadline):
		return errors.New("leader: leader work did not stop within the renew deadline")
	}
}
