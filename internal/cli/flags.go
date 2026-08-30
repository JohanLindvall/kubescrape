package cli

// The flag blocks both binaries register identically. Each main used to
// register its own copy of these, held equal by nothing, and the help text had
// already micro-drifted ("per-export timeout" vs "per-export-attempt timeout",
// an -otlp-insecure hint present on one side only). Registered ONCE here, a
// default or wording edit reaches both binaries by construction; what remains
// per-main is exactly what genuinely differs — the agent's retry/backoff/
// max-send-bytes knobs, the service's repeatable -otlp-header, and the two
// help hints RegisterObsFlags takes as parameters.

import (
	"flag"
	"time"
)

// OTLPFlags holds the values behind the shared -otlp-* exporter flag block.
// The fields are the flag pointers, valid to read after the set is parsed;
// each main maps them onto its own otlpexport.Config (the agent adds its
// retry fields, the service its headers).
type OTLPFlags struct {
	Endpoint           *string
	Protocol           *string
	Compression        *string
	CompressionLevel   *int
	Insecure           *bool
	InsecureSkipVerify *bool
	CAFile             *string
	BearerTokenFile    *string
	Timeout            *time.Duration
}

// RegisterOTLPFlags registers the nine -otlp-* flags shared by both binaries
// on fs. endpointHelp is the one line that legitimately differs: the service's
// exporter carries only its self-metrics and its help says so, while the
// agent's carries every pipeline.
func RegisterOTLPFlags(fs *flag.FlagSet, endpointHelp string) *OTLPFlags {
	return &OTLPFlags{
		Endpoint:           fs.String("otlp-endpoint", "otel-collector.monitoring:4317", endpointHelp),
		Protocol:           fs.String("otlp-protocol", "grpc", "OTLP transport: grpc or http"),
		Compression:        fs.String("otlp-compression", "gzip", "OTLP payload compression: gzip or none"),
		CompressionLevel:   fs.Int("otlp-compression-level", 0, "gzip level 1 (fastest, ~2-3x less CPU for ~10% larger payloads) to 9 (smallest); 0 = library default"),
		Insecure:           fs.Bool("otlp-insecure", true, "use a plaintext gRPC connection (for http, use an http:// endpoint)"),
		InsecureSkipVerify: fs.Bool("otlp-tls-insecure-skip-verify", false, "skip TLS certificate verification towards the collector"),
		CAFile:             fs.String("otlp-tls-ca-file", "", "PEM CA bundle for verifying the collector"),
		BearerTokenFile:    fs.String("otlp-bearer-token-file", "", "file with a bearer token sent on every export (re-read periodically)"),
		Timeout:            fs.Duration("otlp-timeout", 15*time.Second, "per-export-attempt timeout"),
	}
}

// ObsFlags holds the values behind the shared process-observability flag
// block: the metrics and pprof listeners of the three-listener story, the
// self-metrics cadence, and the process logger's level (NewLogger consumes it).
type ObsFlags struct {
	MetricsListen       *string
	PprofListen         *string
	SelfMetricsInterval *time.Duration
	LogLevel            *string
}

// RegisterObsFlags registers the shared observability flags on fs. The two
// hints that genuinely differ between the binaries are parameters: binary
// names the process in the self-metrics help ("service"/"agent"), and
// listenServes names what the binary's own -listen port carries ("the API" /
// "the debug/health surface") in the metrics-listen help.
func RegisterObsFlags(fs *flag.FlagSet, binary, listenServes string) *ObsFlags {
	return &ObsFlags{
		MetricsListen: fs.String("metrics-listen", ":9090",
			"listen address for the Prometheus /metrics endpoint (Go runtime and process metrics; with -self-metrics-interval=0 also the kubescrape_* internal metrics, replacing the OTLP push with a scrape; empty disables). Separate from -listen so "+listenServes+" and the scrape target can be exposed independently"),
		PprofListen: fs.String("pprof-listen", "",
			"listen address for net/http/pprof under /debug/pprof, on its own port (empty disables). Off by default and separate from -listen and -metrics-listen: profiles expose goroutine stacks and heap contents, so this is the port to firewall or bind to localhost"),
		SelfMetricsInterval: fs.Duration("self-metrics-interval", time.Minute,
			"export the "+binary+"'s own metrics over OTLP at this interval (0 disables)"),
		// There is no -log-format. The process logs logfmt, always: one format
		// means every consumer parses the same bytes, and it means the format's
		// correctness is a property internal/cli holds (see NewLogfmtHandler)
		// rather than a per-deployment choice between two half-checked ones.
		LogLevel: fs.String("log-level", "info", "log level: debug, info, warn, error. Output is logfmt (key=value), always — there is no format flag"),
	}
}
