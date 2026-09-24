// Package promscrape is the agent's Prometheus scraper: it fetches the targets
// the metadata service derives for this node (annotation-discovered pods and
// Services, ServiceMonitor and PodMonitor endpoints), scrapes them on their own
// cadence with a constant-memory streaming parser (pkg/promparse), converts the
// exposition — classic and OpenMetrics text, and the protobuf exposition for
// native histograms — into OTLP metrics under the resources internal/agent/attrs
// builds, and exports them in size- and count-bounded chunks.
//
// The kubelet's three endpoints live here too (kubelet.go), because they share
// the scrape machinery and, more importantly, the identity path
// (metaresolve.go): /metrics/cadvisor rows are attributed to pods and
// containers through the cgroup path in their id label (cadvisorbatch.go),
// /metrics is a node-level scrape, and /stats/summary (summary.go) is JSON
// rather than exposition but fills its resources through the same
// fillIdentityResource a cadvisor row goes through, so the two join on
// container.id. Per-pipeline keep/drop rules (filter.go) and
// KSM-style splitters (split.go, splitbatch.go) shape what is exported;
// metabudget.go bounds what a metadata-service outage may cost a scrape.
//
// Both exposition fronts — the text parser and the protobuf decoder
// (protoparse.go) — feed one converter (convert.go), which groups histogram
// and summary component series into points and hands them to a batcher: the
// plain one (batch.go), the split one or the cadvisor one. Every batcher, the
// /stats/summary one included, differs only in which resource a point lands
// on; the point writers and the size model that keeps a chunk under the
// collector's receive limit are shared (otlppoint.go).
package promscrape
