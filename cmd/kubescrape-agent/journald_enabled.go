//go:build journald

package main

import (
	"context"

	"github.com/JohanLindvall/kubescrape/internal/agent/journald"
	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// journaldBuilt reports that this build contains the journal reader: the
// `journald` build tag is set. See buildtags.go for the pair.
const journaldBuilt = true

// startJournald starts the systemd journal reader.
func (p *pipelines) startJournald(ctx context.Context) error {
	if !*journaldOn {
		return nil
	}
	jr := journald.New(journald.Config{
		Dir:           *journaldDir,
		Units:         cli.SplitList(*journaldUnits),
		Positions:     p.posStore,
		BatchSize:     *journaldBatch,
		MaxBatchBytes: *journaldBytes,
		FlushInterval: *journaldFlush,
		MaxEntryBytes: *maxEntryBytes,
		Enrich:        *enrichOn,
		LogAttrs:      p.logAttrs,
		Scrub:         p.scrub,
		Rules:         p.journalRules,
		LogMetrics:    p.logMetrics,
		Attrs:         p.attrBuilders.Journal,
		NodeInfo:      p.nodeInfo,
		Exporter:      p.out,
		Logger:        p.log,
	})
	p.spawn(func() {
		jr.Run(ctx)
	})
	p.log.Info("journald reader started", "dir", *journaldDir, "units", *journaldUnits, "positionsFile", *positionsFile)
	return nil
}
