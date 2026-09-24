package main

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
)

// TestMain fails the package when a test leaves a flag VALUE changed.
//
// This package's tests configure the agent the only way run() reads its
// configuration — by writing through the process-global flag pointers — and
// each one restores what it wrote by hand. A missed restore does not fail the
// test that made it; it silently reconfigures every test that runs after it,
// so results depend on test ORDER (-run filters, -shuffle, a new test added
// earlier in a file). restoreSummaryFlags forgot -cadvisor, which
// TestEffectiveIdentityFollowsTheDeploymentRole turns off, and nothing
// noticed. This is the check for the whole class rather than that instance.
//
// go test's own flags (test.*) are skipped, and flag.Parse runs first so they
// hold their final values before the snapshot. The TestCheckConfigChild half
// of TestCheckConfigExitStatus re-parses the command line inside the child and
// exits from the test itself, so it never reaches the comparison.
func TestMain(m *testing.M) {
	flag.Parse()
	before := flagValues()
	code := m.Run()
	if code == 0 {
		after := flagValues()
		var leaked []string
		for name, was := range before {
			if now, ok := after[name]; ok && now != was {
				leaked = append(leaked, fmt.Sprintf("-%s: %q -> %q", name, was, now))
			}
		}
		if len(leaked) > 0 {
			slices.Sort(leaked)
			fmt.Fprintf(os.Stderr, "FAIL: tests left %d flag value(s) changed; later tests then run against a reconfigured agent (restore what a test writes, in t.Cleanup):\n  %s\n",
				len(leaked), strings.Join(leaked, "\n  "))
			code = 1
		}
	}
	os.Exit(code)
}

// flagValues is every application flag's current value, by name.
func flagValues() map[string]string {
	out := map[string]string{}
	flag.VisitAll(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, "test.") {
			out[f.Name] = f.Value.String()
		}
	})
	return out
}
