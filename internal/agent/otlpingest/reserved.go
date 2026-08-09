package otlpingest

// Reserved-attribute stripping at first receipt.
//
// kubescrape reserves a few attribute keys as its own plumbing, and their
// consumers are PRESENCE-ONLY — neither can tell kubescrape's own mark from a
// sender's. The namespace router honors its script marker on a RESOURCE
// before any namespace glob, so a wire-supplied copy selects any configured
// route and that route's tenant headers; the transform engine's post-script
// prune deletes any marked ELEMENT whenever a script for its signal is
// active, and counts the deletion as an operator-intended
// kubescrape_transform_dropped_total. On listeners nothing authenticates, the
// wire copy must therefore die at first receipt — the application-facing hop,
// the same seam where enrichment and peer attribution happen. The trace
// tier's INTERNAL receiver is deliberately not on this path: what arrives
// there is kubescrape-to-kubescrape and was sanitized when an application
// pushed it.
//
// WHICH keys are reserved is the caller's knowledge (ServerConfig.
// ReservedAttrs, wired in cmd/kubescrape-agent), exactly as RejectTraces
// keeps the loop marker's: this package must not know route's or transform's
// spellings.

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// ReservedAttrs names the attribute keys reserved for kubescrape's own
// plumbing, stripped from every accepted payload at first receipt (both
// transports, all three signals; counted per removed occurrence into
// kubescrape_ingest_reserved_stripped_total{key}, warned throttled per key).
// The zero value strips nothing, which is what any receiver outside the two
// application-facing ones wants.
type ReservedAttrs struct {
	// Resource keys are stripped from every resource's attributes — the
	// router reads its script marker there and nowhere else.
	Resource []string
	// Element keys are stripped from log-record attributes, span attributes,
	// metric Metadata and every data point's attributes across all five
	// metric types — everywhere the transform prune reads its drop marker.
	Element []string
}

func (r ReservedAttrs) empty() bool { return len(r.Resource) == 0 && len(r.Element) == 0 }

// reservedWarnEvery is the per-key re-warn cadence: the condition is a state
// (a sender that ships a reserved key ships it on every push), and the useful
// information is one line naming the key.
const reservedWarnEvery = time.Minute

// stripReserved removes each reserved key present in m, counting and (per
// key, throttled) warning every removal. keys is the operator-wired list, so
// a clean map costs one probe per configured key and allocates nothing — the
// element walk runs per record/point/span and must stay free.
func (s *Server) stripReserved(m pcommon.Map, keys []string) {
	for _, k := range keys {
		if !m.Remove(k) {
			continue
		}
		obs.IngestReservedStripped.WithLabelValues(k).Inc()
		if allow, _ := s.reservedWarns.Allow(k); allow {
			s.log.Warn("ingest: a sender shipped an attribute reserved for kubescrape's own plumbing; stripped at receipt so wire data cannot steer routing or masquerade as an operator-intended drop",
				"key", k)
		}
	}
}

// sanitizeLogs strips the reserved plumbing keys from a pushed logs payload.
func (s *Server) sanitizeLogs(ld plog.Logs) {
	ra := s.cfg.ReservedAttrs
	if ra.empty() {
		return
	}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		s.stripReserved(rl.Resource().Attributes(), ra.Resource)
		if len(ra.Element) == 0 {
			continue // the per-record walk is only worth taking for a key it can find
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				s.stripReserved(lrs.At(k).Attributes(), ra.Element)
			}
		}
	}
}

// sanitizeTraces strips the reserved plumbing keys from a pushed traces
// payload. It runs AFTER the RejectTraces guard — a refused payload needs no
// sanitizing — and before enrichment, like its siblings.
func (s *Server) sanitizeTraces(td ptrace.Traces) {
	ra := s.cfg.ReservedAttrs
	if ra.empty() {
		return
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		s.stripReserved(rs.Resource().Attributes(), ra.Resource)
		if len(ra.Element) == 0 {
			continue
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				s.stripReserved(sps.At(k).Attributes(), ra.Element)
			}
		}
	}
}

// sanitizeMetrics strips the reserved plumbing keys from a pushed metrics
// payload: resource attributes, each metric's Metadata and every data point's
// attributes (the three places a marker could ride a metric).
func (s *Server) sanitizeMetrics(md pmetric.Metrics) {
	ra := s.cfg.ReservedAttrs
	if ra.empty() {
		return
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		s.stripReserved(rm.Resource().Attributes(), ra.Resource)
		if len(ra.Element) == 0 {
			continue
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				s.sanitizeMetricElements(ms.At(k), ra.Element)
			}
		}
	}
}

// sanitizeMetricElements strips Element keys from one metric's Metadata and
// from every data point's attributes — the transform prune reads its marker
// in both places (a marked metric prunes whole, a marked point alone).
func (s *Server) sanitizeMetricElements(m pmetric.Metric, keys []string) {
	s.stripReserved(m.Metadata(), keys)
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	}
}
