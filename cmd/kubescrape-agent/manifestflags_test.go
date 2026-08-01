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
