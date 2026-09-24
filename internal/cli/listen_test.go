package cli

import (
	"strings"
	"testing"
)

// The address comparison decides whether a refusal fires, so its edges are
// pinned: a wildcard host contends with every address on its port, one port
// spelled two ways is one port, and two DIFFERENT hosts on one port do not
// contend at all (refusing those would make the dry run stricter than a start
// that binds them happily).
func TestSameListenAddr(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{":4317", ":4317", true},
		{":4317", "0.0.0.0:4317", true},     // "" and 0.0.0.0 are one wildcard
		{":9090", "0.0.0.0:9090", true},     // the service compared raw strings and passed this
		{"0.0.0.0:4317", "[::]:4317", true}, // both wildcards
		{"127.0.0.1:4317", ":4317", true},   // the wildcard covers loopback
		{":04317", ":4317", true},           // one port, two spellings
		{":09090", ":9090", true},
		{" :9090", ":9090", true},
		{"127.0.0.1:4317", "10.0.0.1:4317", false},
		{":4317", ":4318", false},
		{"", ":4317", false},     // empty disables the listener
		{"4317", ":4317", false}, // not host:port: CheckListeners refuses it by name
	} {
		if got := SameListenAddr(tc.a, tc.b); got != tc.want {
			t.Errorf("SameListenAddr(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
		if got := SameListenAddr(tc.b, tc.a); got != tc.want {
			t.Errorf("SameListenAddr(%q, %q) = %v, want %v (it must be symmetric)", tc.b, tc.a, got, tc.want)
		}
	}
}

// Both binaries' dry runs refuse through CheckListeners, so its two refusals
// must each name the flag an operator would change: net.Listen's own error
// names the value and never the flag.
func TestCheckListenersRefusesByFlagName(t *testing.T) {
	for _, tc := range []struct {
		name string
		ls   []Listener
		want []string // substrings; nil = accepted
	}{
		{"defaults", []Listener{{Flag: "-listen", Addr: ":8080"}, {Flag: "-metrics-listen", Addr: ":9090"}, {Flag: "-pprof-listen"}}, nil},
		{"all disabled", []Listener{{Flag: "-listen"}, {Flag: "-metrics-listen"}, {Flag: "-pprof-listen"}}, nil},
		{"different hosts, one port", []Listener{{Flag: "-a", Addr: "127.0.0.1:9090"}, {Flag: "-b", Addr: "10.0.0.1:9090"}}, nil},
		{"unparseable", []Listener{{Flag: "-listen", Addr: ":8080"}, {Flag: "-pprof-listen", Addr: "nonsense"}},
			[]string{"-pprof-listen", `"nonsense"`, "not a listen address"}},
		{"one address twice", []Listener{{Flag: "-metrics-listen", Addr: ":9090"}, {Flag: "-pprof-listen", Addr: ":9090", Note: PprofNote}},
			[]string{"-metrics-listen", "-pprof-listen", `are both ":9090"`, "address already in use", "goroutine stacks"}},
		{"one socket spelled two ways", []Listener{{Flag: "-listen", Addr: ":9090"}, {Flag: "-metrics-listen", Addr: "0.0.0.0:9090"}},
			[]string{"-listen", "-metrics-listen", `":9090"`, `"0.0.0.0:9090"`, "one socket"}},
		{"zero-padded port", []Listener{{Flag: "-metrics-listen", Addr: ":09090"}, {Flag: "-pprof-listen", Addr: ":9090"}},
			[]string{"-metrics-listen", "-pprof-listen"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckListeners(tc.ls)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a layout the bind refuses")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %q", err, w)
				}
			}
		})
	}
}
