package cgroupstats

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// The two snapshots this file's tests are made of, both taken in init() because
// obs.Registry is a PROCESS GLOBAL and only init() runs before every test in
// the package has had a chance to move it.
//
// The spec runs every package-level variable initialiser FIRST and every init()
// afterwards, so the first observes the state a plain IMPORT leaves behind —
// which is exactly what an agent with -cgroup-stats off has — and the second
// the state ONE New leaves behind, with nothing else having run.
//
// The second one is not a convenience. Deriving it from the live registry
// inside the test is what made this file's second test assert its own name
// falsely for the second time: by then cgroupstats_test.go, discover_test.go
// and distribution_test.go have run (files are compiled in name order, and this
// one sorts after all three) and have FIRED several of these counters for real,
// so "the family has a point" no longer says anything about whether New
// published it. Measured, with newCounters' Add(0) loop deleted: `-run
// TestASamplerPublishesEveryOutcomeItCanProduce` named all five unpublished
// families, `go test ./internal/agent/cgroupstats/` named two — so the test
// guarded two of the five families it advertises in the configuration CI
// actually runs.
var (
	cgroupSeriesAtInit   []string
	cgroupPointsAfterNew map[string][]string
	cgroupSnapshotErr    error
)

func init() {
	cgroupSeriesAtInit = flatten(cgroupPoints())
	cgroupSnapshotErr = buildSnapshotSampler()
	cgroupPointsAfterNew = cgroupPoints()
}

// cgroupPoints maps every kubescrape_cgroup_* family obs REGISTERS to the
// series it currently publishes. A registered family that publishes nothing is
// present with no entries, which is the distinction both tests turn on.
func cgroupPoints() map[string][]string {
	out := map[string][]string{}
	for _, s := range obs.Registry.Dump() {
		if !strings.HasPrefix(s.Name, "kubescrape_cgroup_") {
			continue
		}
		pts := out[s.Name]
		for _, p := range s.Points {
			lbl := ""
			for _, kv := range p.Labels {
				lbl += " " + kv[0] + "=" + kv[1]
			}
			pts = append(pts, s.Name+lbl)
		}
		out[s.Name] = pts
	}
	return out
}

func flatten(m map[string][]string) []string {
	var out []string
	for _, pts := range m {
		out = append(out, pts...)
	}
	slices.Sort(out)
	return out
}

// buildSnapshotSampler constructs exactly one Sampler over an empty hierarchy,
// which is all New needs to publish this pipeline's outcomes. It cannot use the
// test harness: it runs from init(), where there is no *testing.T.
func buildSnapshotSampler() error {
	root, err := os.MkdirTemp("", "cgroupstats-registration")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(root) }()
	if err := os.WriteFile(filepath.Join(root, controllersFile), []byte("cpu memory pids\n"), 0o644); err != nil {
		return err
	}
	s, err := New(Config{
		Root:     root,
		Resolver: &fakeResolver{},
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		return err
	}
	s.closeAll()
	return nil
}

// A metric family is published exactly when its feature RUNS, so a published 0
// means "on and finding nothing" and never "off". obs.RegisterCgroupStats says
// so about the two gauges; the counters have to obey the same rule, and they
// did not.
//
// BINDING a label set publishes its series at zero (internal/metrics'
// vec.with → series.materialize — deliberately, so "this outcome exists in this
// process" is visible before the outcome first occurs). The bindings used to
// live in a package-level var block, which runs because cmd/kubescrape-agent
// IMPORTS this package: measured, fifteen kubescrape_cgroup_* series were
// published at 0 by every agent in the fleet, whether or not -cgroup-stats was
// on. An operator alerting on kubescrape_cgroup_read_errors_total, or graphing
// the loss counter, could not tell a healthy sampler from an absent pipeline.
func TestNoCgroupSeriesArePublishedUntilASamplerExists(t *testing.T) {
	if len(cgroupSeriesAtInit) != 0 {
		t.Errorf("importing this package published %d kubescrape_cgroup_* series at 0 with no sampler in the process:\n\t%s\n"+
			"Bind the obs label values in New (see the counters type), not in a package-level var block.",
			len(cgroupSeriesAtInit), strings.Join(cgroupSeriesAtInit, "\n\t"))
	}
}

// And the other half of the same rule: once a sampler EXISTS, every outcome it
// can produce is published at 0 straight away, so a dashboard does not have to
// wait for the first read error to learn that the family exists.
//
// "Every outcome" is checked against the obs registrations rather than against
// a hand-copied list, because a hand-copied list is how this test came to
// assert its own name falsely the first time: it enumerated the LABELED
// outcomes only, while the five unlabeled kubescrape_cgroup_* counters stayed
// unpublished until they first fired — four of them being rare by design, which
// is precisely the absent-versus-healthy ambiguity the labeled half had just
// been fixed for. The derivation below is what makes a new cgroup counter, of
// either shape, fail here until New publishes it.
//
// It reads the init() snapshot rather than the live registry, which is how it
// came to assert its own name falsely the SECOND time; see the var block.
func TestASamplerPublishesEveryOutcomeItCanProduce(t *testing.T) {
	if cgroupSnapshotErr != nil {
		t.Fatalf("building the one-sampler snapshot: %v", cgroupSnapshotErr)
	}
	want := []string{
		`kubescrape_cgroup_read_errors_total file=cpu.stat`,
		`kubescrape_cgroup_read_errors_total file=memory.current`,
		`kubescrape_cgroup_read_errors_total file=memory.stat`,
		`kubescrape_cgroup_unresolved_total outcome=pending`,
		`kubescrape_cgroup_unresolved_total outcome=unreachable`,
		`kubescrape_cgroup_unresolved_total outcome=abandoned`,
		`kubescrape_cgroup_unresolved_total outcome=export`,
		`kubescrape_cgroup_containers_capped_total cap=tracked`,
		`kubescrape_cgroup_containers_capped_total cap=pending`,
		`kubescrape_cgroup_discovery_errors_total scope=root`,
		`kubescrape_cgroup_discovery_errors_total scope=subtree`,
		`kubescrape_cgroup_windows_dropped_total reason=unresolved`,
		`kubescrape_cgroup_windows_dropped_total reason=export_failed`,
		`kubescrape_cgroup_windows_dropped_total reason=too_short`,
		`kubescrape_cgroup_held_windows_total outcome=held`,
		`kubescrape_cgroup_held_windows_total outcome=expired`,
	}
	got := map[string]bool{}
	for _, s := range flatten(cgroupPointsAfterNew) {
		got[s] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("%s is not published by a running sampler", w)
		}
	}

	// The derivation, which is what stops the list above from being the whole
	// of the test: every kubescrape_cgroup_* family obs REGISTERS must have at
	// least one point once a sampler exists. Registration alone publishes
	// nothing (metrics.Registry.Counter, deliberately), so a family with no
	// points here is one an operator cannot tell from a pipeline that is off.
	names := make([]string, 0, len(cgroupPointsAfterNew))
	for name := range cgroupPointsAfterNew {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if len(cgroupPointsAfterNew[name]) == 0 {
			t.Errorf("%s is registered but publishes nothing with a sampler running: "+
				"a rare outcome's family then appears only when it first occurs, so absent and healthy read alike. "+
				"Bind its label values (or, for an unlabeled counter, Add(0)) in newCounters.", name)
		}
	}
}
