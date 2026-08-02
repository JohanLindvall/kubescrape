//go:build azure

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/agent/azurediag"
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
	kafka := azurediag.KafkaConfig{
		Namespace:            *azureNamespace,
		Group:                *azureGroup,
		Start:                *azureStart,
		ConnectionStringFile: *azureConnFile,
		ClientID:             *azureClientID,
		TenantID:             *azureTenantID,
	}
	if *azureTopics != "" {
		for _, t := range strings.Split(*azureTopics, ",") {
			if t = strings.TrimSpace(t); t != "" {
				kafka.Topics = append(kafka.Topics, t)
			}
		}
	}
	if err := kafka.Resolve(); err != nil {
		return fmt.Errorf("azure diagnostics: %w", err)
	}
	p.ready.require(gateAzure)
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
		Ready:        func() { p.ready.done(gateAzure) },
	})
	p.spawn(func() { reader.Run(ctx) })
	p.log.Info("azure diagnostics enabled", "brokers", kafka.Brokers,
		"topics", kafka.Topics, "group", kafka.Group, "start", kafka.Start)
	return nil
}
