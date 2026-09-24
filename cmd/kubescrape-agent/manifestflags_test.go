package main

import (
	"flag"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// Every flag the shipped manifests pass to this binary must exist in its flag
// set. A miss is not a typo an operator can work around — the process refuses
// to start, so the whole DaemonSet CrashLoops. See package manifestcheck.
func TestManifestFlagsAreDefined(t *testing.T) {
	byFile, err := manifestcheck.Flags(manifestcheck.Dirs, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(byFile) == 0 {
		t.Fatal("no agent manifests found; the check would pass vacuously")
	}
	for path, names := range byFile {
		for _, name := range names {
			if flag.Lookup(name) == nil {
				t.Errorf("%s passes -%s, which kubescrape-agent does not define: every pod would exit 2 at startup", path, name)
			}
		}
	}
}

// Every flag the shipped manifests OFFER commented out (`#- -journald`) must
// exist too: uncommenting the line is the documented way to enable those
// pipelines, so an offer naming a renamed flag CrashLoops whoever follows the
// docs, while every check over the live args stays green. See
// manifestcheck.OfferedFlags.
func TestManifestFlagOffersAreDefined(t *testing.T) {
	byFile, err := manifestcheck.OfferedFlags(manifestcheck.Dirs, true)
	if err != nil {
		t.Fatal(err)
	}
	// deploy/ carries every opt-in per-node pipeline as a commented offer, so a
	// scan that finds none has stopped matching the form rather than found a
	// tree with nothing to check.
	if len(byFile) == 0 {
		t.Fatal("no commented-out flag offers found in the agent manifests; the check would pass vacuously")
	}
	for path, names := range byFile {
		for _, name := range names {
			if flag.Lookup(name) == nil {
				t.Errorf("%s offers -%s (commented out), which kubescrape-agent does not define: "+
					"uncommenting it, as the docs say to, would make every pod exit 2 at startup", path, name)
			}
		}
	}
}

// The same assertion over the chart's RENDERED goldens (one per value
// fixture). The source scan above reads templates line by line, so a flag a
// template emits from a range loop or a computed define never reaches it; the
// render is what the pod actually receives. A fixture that turns a pipeline on
// is the only place its flags are rendered at all.
func TestRenderedChartFlagsAreDefined(t *testing.T) {
	byFile, err := manifestcheck.Flags(manifestcheck.RenderedDirs, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(byFile) == 0 {
		t.Fatal("no agent documents found in the chart goldens; the check would pass vacuously")
	}
	for path, names := range byFile {
		for _, name := range names {
			if flag.Lookup(name) == nil {
				t.Errorf("%s renders -%s, which kubescrape-agent does not define: every pod would exit 2 at startup", path, name)
			}
		}
	}
}
