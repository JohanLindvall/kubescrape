//go:build azure

package main

import (
	"context"
	"fmt"

	"github.com/JohanLindvall/kubescrape/internal/agent/azurediag"
	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// azureBuilt reports that this build contains the Azure diagnostics consumer:
// the `azure` build tag is set. See buildtags.go for the pair.
const azureBuilt = true

// gateAzure is satisfied by the first successful Event Hubs poll (the group
// is joined and the namespace reachable).
const gateAzure = "azure-eventhub"

// validateAzureFlags checks the -azure-* flag surface, before anything is
// acquired. Here rather than in run() because it needs the azurediag package,
// which a build without the `azure` tag does not link.
func validateAzureFlags() error {
	if err := azurediag.ValidateStartMode(*azureStart); err != nil {
		return err
	}
	if *azureOn && *azureNamespace == "" && *azureConnFile == "" {
		return fmt.Errorf("-azure-diagnostics is set but neither -azure-eventhub-namespace nor -azure-eventhub-connection-string-file is")
	}
	// The SHAPE half of the multi-source rules, so -check-config catches it.
	// The rest of ResolveSources reads the connection-string files, which a
	// dry run must not do.
	if n, f := len(cli.SplitList(*azureNamespace)), len(cli.SplitList(*azureConnFile)); f > 0 && n > 1 {
		return fmt.Errorf("-azure-eventhub-namespace lists %d namespaces alongside %d connection strings: a connection string names its own namespace in its Endpoint, so give at most one namespace (as an override) or none", n, f)
	}
	return nil
}

// startAzure starts the Azure diagnostics consumer. Cluster-scoped like
// -events (run it in the same singleton Deployment), but with NO leader
// election: the Kafka consumer group is its coordination, so replicas > 1
// simply share partitions.
func (p *pipelines) startAzure(ctx context.Context) error {
	if !*azureOn {
		return nil
	}
	sources, err := azurediag.ResolveSources(azurediag.SourceSpec{
		Namespaces:            cli.SplitList(*azureNamespace),
		Topics:                cli.SplitList(*azureTopics),
		Group:                 *azureGroup,
		Start:                 *azureStart,
		ConnectionStringFiles: cli.SplitList(*azureConnFile),
		ClientID:              *azureClientID,
		TenantID:              *azureTenantID,
	}, p.log)
	if err != nil {
		return fmt.Errorf("azure diagnostics: %w", err)
	}
	// One Reader per source: each owns its kgo client, its poll/export/commit
	// loop and its offsets, and they share the compiled chain and the
	// exporter exactly as the other pipelines already do concurrently.
	for _, kafka := range sources {
		reader := azurediag.New(azurediag.Config{
			Kafka:        kafka,
			MetricPrefix: *azurePrefix,
			Enrich:       *enrichOn,
			Scrub:        p.scrub,
			LogAttrs:     p.logAttrs,
			Rules:        p.journalRules, // the same logs.rules chain
			LogMetrics:   p.logMetrics,
			Attrs:        p.attrBuilders.Ingest,
			Exporter:     p.out,
			Logger:       p.log,
			// A gate per source, named for what it consumes: one hub nobody
			// may read keeps /readyz honest AND says which one, instead of
			// being masked by a sibling that polled first.
			Ready: p.ready.gate(azureGateName(sources, kafka)),
		})
		p.spawn(func() { reader.Run(ctx) })
		p.log.Info("azure diagnostics enabled", "brokers", kafka.Brokers,
			"topics", kafka.Topics, "group", kafka.Group, "start", kafka.Start)
	}
	return nil
}

// azureGateName is the readiness gate for one source: the bare name while
// there is only one (the historical gate, which deploy manifests and runbooks
// refer to), qualified by namespace and topics once there are several.
func azureGateName(all []azurediag.KafkaConfig, k azurediag.KafkaConfig) string {
	if len(all) < 2 {
		return gateAzure
	}
	return gateAzure + "[" + k.SourceName() + "]"
}
