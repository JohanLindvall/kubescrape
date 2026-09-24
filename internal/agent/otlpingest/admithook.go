package otlpingest

// The operator's ingest admission hook (ServerConfig.Admit — the transforms
// file's ingest: section), applied per pushed resource on all three signals.
// Not admission CONTROL (admit.go), which bounds what the receiver holds.

import (
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// admitLogs/admitMetrics/admitTraces apply the operator's ingest admission
// hook (ServerConfig.Admit — the transforms file's ingest: section) per
// pushed RESOURCE, AFTER the reserved strip and BEFORE enrichment: a rejected
// resource is removed and counted, the push is still acked, and an emptied
// payload acks without a send.
//
// Before enrichment deliberately — a rejected sender must not spend a metadata
// lookup per resource on its way out (the same argument as RejectTraces). AFTER
// the strip equally deliberately, and it is the half that was wrong: the hook
// was reading the sender's unverified identity claim one line before the
// receiver deleted it, so an operator policy keyed on k8s.namespace.name gated
// nothing at all. The strip costs no lookup, so putting it first preserves the
// pre-enrichment argument intact. See ServerConfig.Admit for what the hook can
// therefore key on.
func (s *Server) admitLogs(ld plog.Logs) {
	if s.cfg.Admit == nil {
		return
	}
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		if s.cfg.Admit(rl.Resource().Attributes()) {
			return false
		}
		obs.IngestAdmissionRejected.Inc()
		return true
	})
}

func (s *Server) admitMetrics(md pmetric.Metrics) {
	if s.cfg.Admit == nil {
		return
	}
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		if s.cfg.Admit(rm.Resource().Attributes()) {
			return false
		}
		obs.IngestAdmissionRejected.Inc()
		return true
	})
}

func (s *Server) admitTraces(td ptrace.Traces) {
	if s.cfg.Admit == nil {
		return
	}
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		if s.cfg.Admit(rs.Resource().Attributes()) {
			return false
		}
		obs.IngestAdmissionRejected.Inc()
		return true
	})
}
