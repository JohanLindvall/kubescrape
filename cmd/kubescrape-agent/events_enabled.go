//go:build events

package main

// The real Kubernetes-events pipeline. This file is the ONLY thing in the
// agent that names internal/agent/events and internal/leader, and through them
// k8s.io/client-go — which is the whole point of the tag (see buildtags.go).

import (
	"context"
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
		return fmt.Errorf("events: no namespace for the lease and position ConfigMap; set -events-lease-namespace or $POD_NAMESPACE (downward API)")
	}
	reader := events.New(events.Config{
		Client:          client,
		Positions:       &events.ConfigMapStore{Client: client, Namespace: ns, Name: *eventsConfigMap},
		StartMode:       *eventsStart,
		Namespace:       *eventsNamespace,
		BatchSize:       *eventsBatch,
		FlushInterval:   *eventsFlush,
		PersistInterval: *eventsPersist,
		Meta:            p.meta,
		Enrich:          *enrichOn,
		Scrub:           p.scrub,
		LogAttrs:        p.logAttrs,
		Rules:           p.journalRules, // the same logs.rules chain
		LogMetrics:      p.logMetrics,
		Attrs:           p.attrBuilders.Ingest,
		Exporter:        p.out,
		Logger:          p.log,
	})
	p.spawn(func() {
		// The election goroutine must be inside the WaitGroup: ReleaseOnCancel
		// only hands the lease back if Run returns before the process exits.
		err := leader.Run(ctx, leader.Config{
			Client:    client,
			Namespace: ns,
			Name:      *eventsLease,
			OnStarted: reader.Run,
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
