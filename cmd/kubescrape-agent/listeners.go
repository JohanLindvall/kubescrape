package main

// The addresses this process binds, derived once from the flags: the collision
// check (listenersDistinct) and the startup summary's "effective listeners"
// line both range processListeners.

import (
	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// listenAddr is one address this process will bind, with the flag that named it
// and the key the startup summary's "effective listeners" line prints it under.
type listenAddr struct {
	flag string
	// key is the logfmt key — fixed strings, NOT derived from the flag name,
	// because renaming one renames what operators grep for.
	key  string
	addr string
	// note is appended to a collision message this listener is part of, where
	// losing the bind race is not the only thing wrong with the pair.
	note string
}

// tierListeners are the addresses this process will bind for the trace tier, in
// the order the flags are documented. The application ports are listed only when
// they are actually served (-service-graph-ingest): a dry run that refused a
// collision with a listener the start never binds would be stricter than the
// start, which CrashLoops just as hard as being laxer.
func tierListeners() []listenAddr {
	// The internal hop carries the note because a collision involving it is the
	// one that means more than a lost race: an internal hop addressed to an
	// application port would also re-enrich and re-shard on every pass.
	const internalNote = " The internal hop and the application ports must be different ports — an internal hop addressed to an application port would also re-enrich and re-shard on every pass."
	out := []listenAddr{
		{flag: "-service-graph-listen", key: "serviceGraphInternalGRPC", addr: *serviceGraphListen, note: internalNote},
		{flag: "-service-graph-http-listen", key: "serviceGraphInternalHTTP", addr: *serviceGraphHTTPListen, note: internalNote},
	}
	if *serviceGraphIngest {
		out = append(out,
			listenAddr{flag: "-service-graph-ingest-grpc", key: "serviceGraphIngestGRPC", addr: *serviceGraphIngestGRPC},
			listenAddr{flag: "-service-graph-ingest-http", key: "serviceGraphIngestHTTP", addr: *serviceGraphIngestHTTP})
	}
	return out
}

// processListeners are ALL the addresses this process will bind, given the flags
// as they stand — the health/debug port, the two observability ports, the ingest
// pair when -ingest is on, and the tier's up to four when -service-graph is.
// The ONE list: the collision check (listenersDistinct) and the startup
// summary's "effective listeners" line both range it, so a listener added here
// is refused on collision AND reported, and one cannot be added to only one.
//
// Every listener, not just the tier's, because the collision this refuses is not
// a tier property: -ingest and -service-graph-ingest default to the SAME
// :4317/:4318 (one is the node agent's logs-and-metrics receiver, the other the
// tier's trace receiver), and -pprof-listen typed onto -metrics-listen's :9090
// is the same mistake with no feature flag involved at all. Nothing composes
// them today in a shipped manifest, which is exactly why an operator who does
// deserves the dry run rather than a restart loop.
func processListeners() []listenAddr {
	out := []listenAddr{
		{flag: "-listen", key: "listen", addr: *listen},
		{flag: "-metrics-listen", key: "metricsListen", addr: *metricsListen},
		{flag: "-pprof-listen", key: "pprofListen", addr: *pprofListen, note: cli.PprofNote},
	}
	if *ingestOn {
		out = append(out,
			listenAddr{flag: "-ingest-grpc-endpoint", key: "ingestGRPC", addr: *ingestGRPC},
			listenAddr{flag: "-ingest-http-endpoint", key: "ingestHTTP", addr: *ingestHTTP})
	}
	if *serviceGraphOn {
		out = append(out, tierListeners()...)
	}
	return out
}

// listenersDistinct refuses a listener address that is not host:port and two
// of this process's listeners configured on one socket (cli.CheckListeners,
// the rule the metadata service's dry run applies to its own three).
//
// The tier alone binds up to four, from four independent flags — and the chart
// renders three of them from values, so `serviceGraph.port: 4317` (warned
// against in values.yaml prose, enforced nowhere) puts the INTERNAL receiver on
// the application gRPC port. The servers start concurrently, so whichever binds
// second dies with `address already in use` and takes the process with it; which
// one that is varies between restarts. Loud, but only at the real start —
// refused here, beside the ring cross-check, because this is the place that
// knows more than one listener exists.
func listenersDistinct() error {
	ls := processListeners()
	out := make([]cli.Listener, len(ls))
	for i, l := range ls {
		out[i] = cli.Listener{Flag: l.flag, Addr: l.addr, Note: l.note}
	}
	return cli.CheckListeners(out)
}
