//go:build events

package main

// The real Kubernetes-events pipeline. This file is the ONLY thing in the
// agent that names internal/agent/events and internal/leader, and through them
// k8s.io/client-go — which is the whole point of the tag (see buildtags.go).

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/client-go/kubernetes"

	"github.com/JohanLindvall/kubescrape/internal/agent/events"
	"github.com/JohanLindvall/kubescrape/internal/cli/kubecfg"
	"github.com/JohanLindvall/kubescrape/internal/leader"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// eventsBuilt reports that this build contains the Kubernetes events reader.
const eventsBuilt = true

// validateEventsFlags checks the -events-* surface that can be judged without
// a cluster. Its counterpart in the stub has nothing to check, exactly as the
// azure pair does: -events-start describes a pipeline a tag-less binary
// refuses to start at all.
func validateEventsFlags() error { return events.ValidateStartMode(*eventsStart) }

// startEvents starts the cluster-singleton Kubernetes events reader under a
// leader election, so exactly one replica watches (N watchers would emit N
// copies of every event).
func (p *pipelines) startEvents(ctx context.Context) error {
	if !*eventsOn {
		return nil
	}
	cfg, err := kubecfg.KubeConfig(*kubeconfig)
	if err != nil {
		return fmt.Errorf("events: building the kubernetes client config: %w", err)
	}
	cfg.UserAgent = "kubescrape-agent"
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("events: creating the kubernetes client: %w", err)
	}
	ns := *eventsLeaseNS
	if ns == "" {
		ns = leader.Namespace()
	}
	if ns == "" {
		return errors.New("events: no namespace for the lease and position ConfigMap; set -events-lease-namespace or $POD_NAMESPACE (downward API)")
	}
	// ONE renew deadline for the election AND the reader's stop budget: the
	// reader's final flush and position write must finish inside the window
	// the election gives leader work to stop in (events.Config.StopBudget), so
	// the budget is derived from the deadline rather than left to a second
	// default that merely agrees with it today.
	renew := leader.DefaultRenewDeadline
	reader := events.New(events.Config{
		Client:          client,
		Positions:       &events.ConfigMapStore{Client: client, Namespace: ns, Name: *eventsConfigMap},
		StartMode:       *eventsStart,
		Namespace:       *eventsNamespace,
		BatchSize:       *eventsBatch,
		FlushInterval:   *eventsFlush,
		PersistInterval: *eventsPersist,
		StopBudget:      renew / 2,
		Meta:            p.meta,
		Chain:           p.logChain(),
		// The INGEST attribute pipeline (there is no events-specific one),
		// like -azure-diagnostics. So resourceAttributes.pipelines.ingest
		// governs every event resource, and a pipelines.logs override (of
		// service.name, say) does NOT reach the events about that pod, whose
		// logs it does govern — with the default builders the two agree, so
		// it only matters once an operator sets per-pipeline overrides.
		Attrs:    p.attrBuilders.Ingest,
		Exporter: p.out,
		Logger:   p.log,
	})
	p.spawn(func() {
		// The election goroutine must be inside the WaitGroup: ReleaseOnCancel
		// only hands the lease back if Run returns before the process exits.
		err := leader.Run(ctx, leader.Config{
			Client:        client,
			Namespace:     ns,
			Name:          *eventsLease,
			RenewDeadline: renew,
			OnStarted:     reader.Run,
			OnLeading: func(leading bool) {
				if leading {
					obs.Leader.Set(1)
				} else {
					obs.Leader.Set(0)
				}
			},
			Log: p.log,
		})
		if err != nil {
			p.fatal("events leader election", err)
		}
	})
	p.log.Info("kubernetes events enabled", "lease", *eventsLease, "namespace", ns,
		"positionConfigMap", *eventsConfigMap, "start", *eventsStart)
	return nil
}
