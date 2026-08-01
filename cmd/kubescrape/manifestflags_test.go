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
