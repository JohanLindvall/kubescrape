package main

// The metadata service's pre-flight, and the dry run that runs it: -check-config.
//
// The agent has had one since it grew a config file, and docs/FIRST-RUN.md
// builds its pre-flight around it — so the SERVICE half of an install was the
// one that could not be checked before it was rolled out, which is backwards:
// the service is the singleton every agent in the fleet blocks on.
//
// The discipline is the agent's, verbatim, because it is what makes a dry run
// worth running: ONE function does the refusals, a real start calls it too, and
// it acquires nothing. A check that ran its own copy of the rules would be a
// check that can pass a config the process then CrashLoops on — which is the
// failure it exists to prevent.
//
// Where it deliberately stops is at the ENVIRONMENT. Whether the scrape-auth
// token file is readable, whether a ServiceAccount is projected, whether the
// API server answers: none of that is the config, and all of it is false on the
// machine the pre-flight is run from (FIRST-RUN's is a local binary). The
// agent's `-service-graph-token-file` check draws the same line in the same
// words — shape here, readability at the real start, where it is equally fatal.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/JohanLindvall/kubescrape/internal/cli"
	"github.com/JohanLindvall/kubescrape/internal/server"
)

// validateConfig is every refusal this process makes before it acquires
// anything, plus the warnings that name a legal-but-surprising combination.
// run() calls it and so does -check-config — the same call, which is what keeps
// a dry run from describing a different process than the one that starts.
func validateConfig(log *slog.Logger) error {
	if err := checkFlagValues(); err != nil {
		return err
	}
	if err := checkListenAddrs(); err != nil {
		return err
	}
	if err := checkMonitorNamespaces(*monitorNamespaces); err != nil {
		return err
	}
	// The SERVER's own rule, called rather than restated: /v1/scrape-auth is
	// the one authenticated route, and a refusal that lived here in a second
	// copy could drift from the one the constructor enforces. Shape only —
	// whether the file is readable and non-empty is newScrapeAuthTokens' at the
	// real start, where it is equally fatal.
	if err := scrapeAuthShape().Validate(); err != nil {
		return err
	}
	// Errors AND a warning (the cap is a memory bound wearing a count), so it
	// runs after the refusals and before the rest of the warnings.
	if err := checkWaiterCap(*maxWaiters, log); err != nil {
		return err
	}
	logConfigWarnings(log)
	return nil
}

// checkFlagValues refuses flag values whose only workable meaning is not the
// one the flag reads like — the agent's function of the same name, for this
// binary's surface. Every one of these is silent at runtime: the process starts,
// serves, and does the wrong thing for its whole life.
func checkFlagValues() error {
	// A negative resync is not a mode, it is a typo: client-go treats the
	// period as a deadline that is always in the past, so every informer
	// resyncs continuously — replaying UpsertPod for every pod in the cluster
	// under the store's write lock, in a loop, with no flag named anywhere in
	// the symptoms. 0 legitimately means "no periodic resync".
	if *resync < 0 {
		return fmt.Errorf("-resync %v: must not be negative (0 disables periodic resync)", *resync)
	}
	// Negative is a typo, not a mode: a ticker refuses it (time.NewTicker
	// panics), and silently reading it as "disabled" would remove the one
	// signal an API-server outage produces without saying so.
	if *apiserverProbeInterval < 0 {
		return fmt.Errorf("-apiserver-probe-interval %v: must not be negative (0 disables the probe)", *apiserverProbeInterval)
	}
	// The three remaining durations, where a negative one is read by its
	// consumer as something quieter than an error and nothing ever names it.
	// Each carries what the process would actually DO with it, because the
	// number alone cannot say.
	for _, d := range []struct {
		flag        string
		value       time.Duration
		consequence string
	}{
		{"wait-timeout", *maxWait,
			"no container lookup would block at all, so the ~1s gap between a container starting and the kubelet posting its ID becomes a miss on the FIRST log line of every container on every node (0 says the same thing on purpose)"},
		{"cache-ttl", *cacheTTL,
			"a deleted pod's tombstone would expire before the sweeper's next pass, so a draining pod's last logs lose their attribution (0 says the same thing on purpose)"},
		{"metadata-cache-ttl", *metaCacheTTL,
			"the response cache headers are simply off — the handlers read any non-positive TTL that way — so every agent re-fetches every document each cycle instead of revalidating it with a 304 (0 is the documented spelling of that)"},
	} {
		if d.value < 0 {
			return fmt.Errorf("-%s %v: a negative duration is a typo rather than a mode — %s", d.flag, d.value, d.consequence)
		}
	}
	return nil
}

// checkListenAddrs refuses the two listener mistakes that produce a RUNNING pod
// serving nothing an operator can reach.
//
// An address already in use names itself at startup and is fatal, so it needs
// no check. These two do not: an unparseable address fails inside
// net.Listen with a message that names neither the flag nor what was wrong with
// the value, and an EMPTY -listen is worse than either — net/http reads it as
// ":http", so the API binds port 80 while every probe, Service and agent in the
// manifests points at 8080. Nothing fails; the pod simply never answers.
func checkListenAddrs() error {
	// Empty is the documented "off" for both observability listeners and is
	// therefore only checked when set; -listen has no off.
	if strings.TrimSpace(*listen) == "" {
		return errors.New("-listen is empty: net/http reads an empty address as \":http\", so the metadata API would bind port 80 " +
			"while the manifests' probes, Service and agents all address the configured port. Pass a host:port (the default is :8080)")
	}
	seen := map[string]string{}
	for _, l := range []struct{ flag, addr string }{
		{"listen", *listen},
		{"metrics-listen", *metricsListen},
		{"pprof-listen", *pprofListen},
	} {
		if l.addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(l.addr); err != nil {
			return fmt.Errorf("-%s %q is not a listen address: %w (want host:port, or :port for every interface)", l.flag, l.addr, err)
		}
		// Two listeners on one address is a bind failure for whichever binds
		// second — which, since the metrics endpoint binds first and is fatal,
		// is usually the API. The three ports exist because they have
		// different exposure profiles (pprof serves goroutine stacks and heap
		// contents), so a collision is never what was meant.
		if other, dup := seen[l.addr]; dup {
			return fmt.Errorf("-%s and -%s are both %q: the second listener to bind fails with 'address already in use'. "+
				"The three ports are separate because they have different exposure profiles — pprof in particular serves goroutine stacks and heap contents", other, l.flag, l.addr)
		}
		seen[l.addr] = l.flag
	}
	return nil
}

// checkMonitorNamespaces refuses an entry that can never match.
//
// -monitor-namespaces is an EXACT set (parseNamespaceSet builds a map and
// monitorAllowed does a lookup), and that is the trap: this repo's other
// namespace flags — the agent's -logs-exclude-namespaces, a log source's
// `namespaces` — are path.Match GLOBS, so `kube-*` here reads like it would
// work. It does not, and the failure is silent in the worst direction: the
// monitors an admin meant to allow are simply never indexed, no target is ever
// served for them, and nothing anywhere says why.
//
// So anything that is not a legal namespace name is refused, and a glob
// metacharacter is refused with its own sentence, since that is the mistake
// that will actually be made.
func checkMonitorNamespaces(s string) error {
	for _, ns := range cli.SplitList(s) {
		if strings.ContainsAny(ns, "*?[") {
			return fmt.Errorf("-monitor-namespaces %q: this flag is an EXACT namespace list, not a glob — the entry would be compared literally and match nothing, "+
				"so every ServiceMonitor and PodMonitor it was meant to allow is silently never indexed. List the namespaces", ns)
		}
		if msgs := validation.IsDNS1123Label(ns); len(msgs) > 0 {
			return fmt.Errorf("-monitor-namespaces %q is not a Kubernetes namespace name (%s), so it can never match and the monitors it was meant to allow are silently never indexed",
				ns, strings.Join(msgs, "; "))
		}
	}
	return nil
}

// scrapeAuthShape mirrors the two flags onto the Config fields Validate reads,
// so the dry run enforces the server's own rule instead of a copy of it. A
// placeholder stands for each half — the check is nil-ness, and a dry run
// acquires neither a Kubernetes client nor the token file (the agent's
// tailSampling.Validate does the same with its script).
func scrapeAuthShape() server.Config {
	var cfg server.Config
	if *scrapeAuthOn {
		cfg.Secrets = placeholderSecrets{}
	}
	if strings.TrimSpace(*scrapeAuthTokenFile) != "" {
		cfg.ScrapeAuthTokens = func() []string { return nil }
	}
	return cfg
}

// placeholderSecrets is a non-nil server.SecretReader that reads nothing. It
// exists only so Validate sees the SHAPE a start would build; reaching its Get
// would mean a validation path had begun serving requests.
type placeholderSecrets struct{}

func (placeholderSecrets) Get(context.Context, string, string, string) (string, error) {
	return "", errors.New("placeholder secret reader: -check-config acquires no Kubernetes client")
}

// logConfigWarnings names the combinations that are legal, start fine, and are
// almost certainly not what was meant. Emitted by -check-config and by every
// real start alike — the discipline validateConfig already holds for the
// refusals, because a dry run and a rollout must not describe different
// processes.
func logConfigWarnings(log *slog.Logger) {
	// -scrape-auth-secrets derives its allowlist from INDEXED monitors, so
	// without -servicemonitors nothing is ever indexed and every request to
	// /v1/scrape-auth 404s ("no monitors indexed") — while the deployment
	// still carries the cluster-wide `secrets: get` grant the flag requires.
	// Warned rather than refused: -servicemonitors with the CRD absent
	// legitimately leaves the index nil too, and that degradation is
	// deliberate, so a hard failure here would refuse a legal configuration.
	if *scrapeAuthOn && !*monitorsOn {
		log.Warn("-scrape-auth-secrets has no effect without -servicemonitors: " +
			"the served allowlist is derived from indexed monitors, so every request will 404 — " +
			"the cluster-wide `secrets: get` grant it requires is unused")
	}
}
