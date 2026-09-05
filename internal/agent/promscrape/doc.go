// Package promscrape is the agent's Prometheus scraper: it fetches the targets
// the metadata service derives for this node (annotation-discovered pods and
// Services, ServiceMonitor and PodMonitor endpoints), scrapes them on their own
// cadence with a constant-memory streaming parser (pkg/promparse), converts the
// exposition — classic and OpenMetrics text, and the protobuf exposition for
// native histograms — into OTLP metrics under the resources internal/agent/attrs
// builds, and exports them in size- and count-bounded chunks.
//
// The kubelet's three endpoints live here too, because they share the scrape
// machinery and, more importantly, the identity path: /metrics/cadvisor rows
// are attributed to pods and containers through the cgroup path in their id
// label (cadvisor.go, cadvisorbatch.go), /metrics is a node-level scrape, and
// /stats/summary (summary.go) is JSON rather than exposition but fills its
// resources through the same FillContainerResource a cadvisor row goes through,
// so the two join on container.id. Per-pipeline keep/drop rules (filter.go) and
// KSM-style splitters (split.go) shape what is exported; metabudget.go bounds
// what a metadata-service outage may cost a scrape.
package promscrape
