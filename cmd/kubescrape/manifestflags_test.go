package main

import (
	"flag"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/manifestcheck"
)

// As for the agent: a flag the manifests pass but the binary does not define
// stops the metadata service from starting at all, and with it every agent's
// attribution. See package manifestcheck.
func TestManifestFlagsAreDefined(t *testing.T) {
	byFile, err := manifestcheck.Flags(manifestcheck.Dirs, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(byFile) == 0 {
		t.Fatal("no metadata-service manifests found; the check would pass vacuously")
	}
	for path, names := range byFile {
		for _, name := range names {
			if flag.Lookup(name) == nil {
				t.Errorf("%s passes -%s, which kubescrape does not define: the service would exit 2 at startup", path, name)
			}
		}
	}
}

// As for the agent's: a flag a manifest OFFERS commented out must exist, since
// uncommenting it is how an operator enables it. The service manifests carry
// no offer today, so there is no vacuity floor here — this is the guard for
// the first one. See manifestcheck.OfferedFlags.
func TestManifestFlagOffersAreDefined(t *testing.T) {
	byFile, err := manifestcheck.OfferedFlags(manifestcheck.Dirs, false)
	if err != nil {
		t.Fatal(err)
	}
	for path, names := range byFile {
		for _, name := range names {
			if flag.Lookup(name) == nil {
				t.Errorf("%s offers -%s (commented out), which kubescrape does not define: "+
					"uncommenting it would make the service exit 2 at startup", path, name)
			}
		}
	}
}

// As for the agent's: the chart's RENDERED goldens carry every flag a template
// emits, including those a range loop or a computed define produces, which the
// line-by-line source scan cannot see.
func TestRenderedChartFlagsAreDefined(t *testing.T) {
	byFile, err := manifestcheck.Flags(manifestcheck.RenderedDirs, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(byFile) == 0 {
		t.Fatal("no metadata-service documents found in the chart goldens; the check would pass vacuously")
	}
	for path, names := range byFile {
		for _, name := range names {
			if flag.Lookup(name) == nil {
				t.Errorf("%s renders -%s, which kubescrape does not define: the service would exit 2 at startup", path, name)
			}
		}
	}
}
