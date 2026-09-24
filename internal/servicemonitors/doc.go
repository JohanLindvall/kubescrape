// Package servicemonitors indexes Prometheus-Operator ServiceMonitor and
// PodMonitor custom resources so their targets can be served alongside
// annotation-discovered ones. Only pod-backed Services are supported: targets
// resolve through the selected Services' pod selectors, which keeps scraping
// node-local.
//
// A documented SUBSET of the CRD is interpreted: endpoint port/targetPort/
// path/scheme, per-endpoint interval/scrapeTimeout, basicAuth, authorization,
// bearerTokenSecret, secret-backed tlsConfig (ca/cert/keySecret/serverName and
// insecureSkipVerify), and the keep/drop subset of metricRelabelings.
// Everything else parsed here exists to be REPORTED as uninterpreted through
// Endpoint.Ignored — see IgnoredFields — because a narrower implementation is
// a choice and a silently partially-applied CR is not.
//
// Endpoint.secretRefs (endpoint.go) is a security boundary: it is the ONE list
// of the endpoint fields carrying secret references, and both the
// monitor-namespacing and the /v1/scrape-auth allowlist derive from it. Adding
// a secret-bearing field means adding it there.
//
// The files, by concern: endpoint.go (the Endpoint model, the CRD endpoint
// decode shape and its conversion), relabel.go (the metricRelabelings walk and
// its ceilings), bounds.go (the endpoint string ceilings), parse.go (the monitor
// kinds and the shared parse skeleton with the monitor-level ceilings),
// podmonitors.go (the PodMonitor kind), index.go (the Index and its change/news
// protocol), authrefs.go (the /v1/scrape-auth allowlist) and report.go (the
// ignored-fields report).
package servicemonitors
