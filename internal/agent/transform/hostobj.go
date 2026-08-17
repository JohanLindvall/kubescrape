package transform

// Lazy Starlark host objects over pdata. A script sees:
//
//	def transform(batch):
//	    for r in batch:            # records / spans / metrics
//	        if r.attributes["level"] == "debug": r.drop()
//	        r.resource["env"] = "prod"
//	        r.body = r.body.replace("secret", "*")   # logs
//
// Views alias the underlying pdata — mutations are in place, nothing is
// materialized unless touched. drop() marks the element; the engine prunes
// marked elements (and emptied groups) after the run.

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.starlark.net/starlark"

	"github.com/JohanLindvall/kubescrape/internal/agent/route"
)

// DropMarker flags an element for post-run pruning. Logs and spans carry it
// as a record/span attribute; metrics carry it in the pdata Metadata map,
// which keeps it OFF the data-point attributes a rename could touch, so a
// drop-then-rename cannot un-drop it. The marker never survives into the
// export regardless — a marked metric is pruned whole and a script error
// discards the transform's whole working copy — so it never reaches the wire
// (the Metadata field IS an OTLP wire field, unlike an earlier claim here).
//
// Exported because the key is RESERVED plumbing, like route.ScriptMarker: the
// prune is presence-only and cannot tell a script's mark from a sender's, so
// a marked element arriving ON THE WIRE would be deleted — and counted as an
// operator-intended kubescrape_transform_dropped_total — whenever any script
// for its signal is active. The ingest receivers strip it at first receipt
// (otlpingest.ServerConfig.ReservedAttrs, wired in cmd/kubescrape-agent), and
// they take the spelling from here so the two cannot drift.
const DropMarker = "__kubescrape_drop__"

// --- attribute map view ---

// attrsView is a dict-like view over a pcommon.Map: get/set/delete of
// string/bool/int/float values.
type attrsView struct{ m pcommon.Map }

func (a attrsView) String() string        { return "attributes" }
func (a attrsView) Type() string          { return "attributes" }
func (a attrsView) Freeze()               {}
func (a attrsView) Truth() starlark.Bool  { return a.m.Len() > 0 }
func (a attrsView) Hash() (uint32, error) { return 0, fmt.Errorf("attributes are unhashable") }

func toStarlark(v pcommon.Value) starlark.Value {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return starlark.String(v.Str())
	case pcommon.ValueTypeBool:
		return starlark.Bool(v.Bool())
	case pcommon.ValueTypeInt:
		return starlark.MakeInt64(v.Int())
	case pcommon.ValueTypeDouble:
		return starlark.Float(v.Double())
	default:
		return starlark.String(v.AsString()) // maps/slices read as their JSON form
	}
}

func fromStarlark(dst pcommon.Value, v starlark.Value) error {
	switch x := v.(type) {
	case starlark.String:
		dst.SetStr(string(x))
	case starlark.Bool:
		dst.SetBool(bool(x))
	case starlark.Int:
		i, ok := x.Int64()
		if !ok {
			return fmt.Errorf("integer out of range")
		}
		dst.SetInt(i)
	case starlark.Float:
		dst.SetDouble(float64(x))
	default:
		return fmt.Errorf("unsupported attribute type %s", v.Type())
	}
	return nil
}

// Get implements attrs["k"]; missing keys return None (not an error), so
// scripts can test `if r.attributes["k"] != None`.
func (a attrsView) Get(k starlark.Value) (starlark.Value, bool, error) {
	key, ok := starlark.AsString(k)
	if !ok {
		return nil, false, fmt.Errorf("attribute key must be a string")
	}
	v, found := a.m.Get(key)
	if !found {
		return starlark.None, true, nil
	}
	return toStarlark(v), true, nil
}

// Has implements `"k" in attrs`.
//
// It MUST exist. Starlark's IN operator tries Container.Has FIRST and only
// falls back to Mapping.Get — and Get deliberately reports found=true for a
// missing key so scripts can write `attrs["k"] != None`. Without Has, every
// membership test answered True, so a script like
//
//	if "debug" in record.attributes: record.drop()
//
// dropped EVERY record. The export then had nothing to send, returned nil,
// and the producer committed its offsets: a node's logs gone with no error
// logged and no counter moved.
func (a attrsView) Has(k starlark.Value) (bool, error) {
	key, ok := starlark.AsString(k)
	if !ok {
		return false, fmt.Errorf("attribute key must be a string")
	}
	_, found := a.m.Get(key)
	return found, nil
}

// SetKey implements attrs["k"] = v; None deletes.
func (a attrsView) SetKey(k, v starlark.Value) error {
	key, ok := starlark.AsString(k)
	if !ok {
		return fmt.Errorf("attribute key must be a string")
	}
	if v == starlark.None {
		a.m.Remove(key)
		return nil
	}
	// Check convertibility BEFORE touching the map, so a failed assignment is a
	// genuine no-op.
	//
	// Two wrong versions preceded this one, and both are easy to re-introduce.
	// PutEmpty adds the key before the value is converted, so returning the
	// error alone left an Empty-valued attribute behind — a partial mutation
	// the fail-open `admit` hook then forwards, contradicting "a hook error did
	// nothing". Removing the key on error is WORSE, not better: PutEmpty has
	// already overwritten whatever was there, so a failed assignment to an
	// EXISTING key deleted it outright — turning a stray attribute into data
	// loss, on a path where the deleted key can be identity (a script assigning
	// a parsed JSON object to service.name). Only a pre-check leaves the map
	// untouched in both cases. It costs one type switch and allocates nothing.
	if err := convertible(v); err != nil {
		return err
	}
	return fromStarlark(a.m.PutEmpty(key), v)
}

// convertible reports whether fromStarlark would accept v, WITHOUT a
// destination to write into. It must stay exhaustive against fromStarlark's
// switch — including the Int64 range check, which is the one failure that is
// not merely a type mismatch.
func convertible(v starlark.Value) error {
	switch x := v.(type) {
	case starlark.String, starlark.Bool, starlark.Float:
		return nil
	case starlark.Int:
		if _, ok := x.Int64(); !ok {
			return fmt.Errorf("integer out of range")
		}
		return nil
	default:
		return fmt.Errorf("unsupported attribute type %s", v.Type())
	}
}

// --- shared element plumbing ---

type dropFn struct{ mark func() }

func (d dropFn) Name() string          { return "drop" }
func (d dropFn) String() string        { return "drop" }
func (d dropFn) Type() string          { return "builtin_function_or_method" }
func (d dropFn) Freeze()               {}
func (d dropFn) Truth() starlark.Bool  { return true }
func (d dropFn) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }
func (d dropFn) CallInternal(*starlark.Thread, starlark.Tuple, []starlark.Tuple) (starlark.Value, error) {
	d.mark()
	return starlark.None, nil
}

// --- log batch ---

type logBatch struct {
	ld plog.Logs
	em MetricEmitter
}

func (b *logBatch) String() string        { return "log_batch" }
func (b *logBatch) Type() string          { return "log_batch" }
func (b *logBatch) Freeze()               {}
func (b *logBatch) Truth() starlark.Bool  { return true }
func (b *logBatch) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

// Iterate walks the batch's (resource, scope, record) positions lazily.
//
// It used to materialize the whole batch into a []*logRecord before the first
// Next, which made "lazy host objects" lazy per FIELD but not per RECORD: a
// 1024-record batch cost 1,037 allocations and 62 KB before the script ran a
// line, and a script that broke out early paid for every record it never saw.
// The walk is now positional, so a break costs only what it consumed.
//
// The per-record view is still freshly allocated rather than reused: Starlark
// hands the value to the loop variable, and a script may keep it past the
// iteration — `[r for r in batch if r.severity_text == "ERROR"]` is the
// obvious way to write a two-pass script, and one reused view would silently
// turn every element of that list into the LAST record.
func (b *logBatch) Iterate() starlark.Iterator {
	return &logIter{rls: b.ld.ResourceLogs(), em: b.em}
}

type logIter struct {
	rls     plog.ResourceLogsSlice
	em      MetricEmitter
	i, j, k int
}

func (it *logIter) Next(v *starlark.Value) bool {
	for it.i < it.rls.Len() {
		rl := it.rls.At(it.i)
		sls := rl.ScopeLogs()
		if it.j >= sls.Len() {
			it.i, it.j, it.k = it.i+1, 0, 0
			continue
		}
		lrs := sls.At(it.j).LogRecords()
		if it.k >= lrs.Len() {
			it.j, it.k = it.j+1, 0
			continue
		}
		*v = &logRecord{lr: lrs.At(it.k), res: rl.Resource(), scope: sls.At(it.j).Scope(), em: it.em}
		it.k++
		return true
	}
	return false
}
func (it *logIter) Done() {}

// logRecord exposes body, severity_text, severity_number, attributes,
// resource and drop().
type logRecord struct {
	lr    plog.LogRecord
	res   pcommon.Resource
	scope pcommon.InstrumentationScope
	em    MetricEmitter
}

func (r *logRecord) String() string        { return "log_record" }
func (r *logRecord) Type() string          { return "log_record" }
func (r *logRecord) Freeze()               {}
func (r *logRecord) Truth() starlark.Bool  { return true }
func (r *logRecord) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (r *logRecord) AttrNames() []string {
	return []string{
		"attributes", "body", "drop", "emit_metric", "observed_time_unix_nano",
		"resource", "route", "scope_name", "severity_number", "severity_text",
		"span_id", "time_unix_nano", "trace_id",
	}
}

func (r *logRecord) Attr(name string) (starlark.Value, error) {
	switch name {
	case "body":
		return starlark.String(r.lr.Body().AsString()), nil
	case "severity_text":
		return starlark.String(r.lr.SeverityText()), nil
	case "severity_number":
		return starlark.MakeInt(int(r.lr.SeverityNumber())), nil
	case "attributes":
		return attrsView{r.lr.Attributes()}, nil
	case "resource":
		return attrsView{r.res.Attributes()}, nil
	case "drop":
		return dropFn{mark: func() { r.lr.Attributes().PutBool(DropMarker, true) }}, nil
	case "route":
		return routeFn(r.res), nil
	case "emit_metric":
		return emitFn(r.res, r.em), nil
	case "time_unix_nano":
		return starlark.MakeInt64(int64(r.lr.Timestamp())), nil
	case "observed_time_unix_nano":
		return starlark.MakeInt64(int64(r.lr.ObservedTimestamp())), nil
	case "trace_id":
		return hexID(r.lr.TraceID().String(), r.lr.TraceID().IsEmpty()), nil
	case "span_id":
		return hexID(r.lr.SpanID().String(), r.lr.SpanID().IsEmpty()), nil
	case "scope_name":
		return starlark.String(r.scope.Name()), nil
	}
	return nil, nil
}

func (r *logRecord) SetField(name string, v starlark.Value) error {
	switch name {
	case "body":
		s, ok := starlark.AsString(v)
		if !ok {
			return fmt.Errorf("body must be a string")
		}
		r.lr.Body().SetStr(s)
		return nil
	case "severity_text":
		s, ok := starlark.AsString(v)
		if !ok {
			return fmt.Errorf("severity_text must be a string")
		}
		r.lr.SetSeverityText(s)
		return nil
	case "severity_number":
		n, ok := v.(starlark.Int)
		if !ok {
			return fmt.Errorf("severity_number must be an int")
		}
		i, _ := n.Int64()
		r.lr.SetSeverityNumber(plog.SeverityNumber(i))
		return nil
	case "time_unix_nano", "observed_time_unix_nano":
		n, ok := v.(starlark.Int)
		if !ok {
			return fmt.Errorf("%s must be an int (unix nanoseconds)", name)
		}
		i, ok := n.Int64()
		if !ok || i < 0 {
			return fmt.Errorf("%s out of range", name)
		}
		if name == "time_unix_nano" {
			r.lr.SetTimestamp(pcommon.Timestamp(i))
		} else {
			r.lr.SetObservedTimestamp(pcommon.Timestamp(i))
		}
		return nil
	}
	return fmt.Errorf("cannot set %s", name)
}

// hexID renders a trace/span id for scripts: the lowercase hex string, or
// None for the zero id (so `if r.trace_id:` reads naturally).
func hexID(hex string, empty bool) starlark.Value {
	if empty {
		return starlark.None
	}
	return starlark.String(hex)
}

// --- trace batch ---

type traceBatch struct {
	td ptrace.Traces
	em MetricEmitter
}

func (b *traceBatch) String() string        { return "trace_batch" }
func (b *traceBatch) Type() string          { return "trace_batch" }
func (b *traceBatch) Freeze()               {}
func (b *traceBatch) Truth() starlark.Bool  { return true }
func (b *traceBatch) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

// Iterate walks the batch positionally — see logBatch.Iterate.
func (b *traceBatch) Iterate() starlark.Iterator {
	return &spanIter{rss: b.td.ResourceSpans(), em: b.em}
}

type spanIter struct {
	rss     ptrace.ResourceSpansSlice
	em      MetricEmitter
	i, j, k int
}

func (it *spanIter) Next(v *starlark.Value) bool {
	for it.i < it.rss.Len() {
		rs := it.rss.At(it.i)
		sss := rs.ScopeSpans()
		if it.j >= sss.Len() {
			it.i, it.j, it.k = it.i+1, 0, 0
			continue
		}
		sps := sss.At(it.j).Spans()
		if it.k >= sps.Len() {
			it.j, it.k = it.j+1, 0
			continue
		}
		*v = &spanObj{sp: sps.At(it.k), res: rs.Resource(), em: it.em}
		it.k++
		return true
	}
	return false
}
func (it *spanIter) Done() {}

type spanObj struct {
	sp  ptrace.Span
	res pcommon.Resource
	em  MetricEmitter
}

func (s *spanObj) String() string        { return "span" }
func (s *spanObj) Type() string          { return "span" }
func (s *spanObj) Freeze()               {}
func (s *spanObj) Truth() starlark.Bool  { return true }
func (s *spanObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (s *spanObj) AttrNames() []string {
	return []string{
		"attributes", "drop", "duration_ms", "emit_metric", "kind", "name",
		"resource", "route", "span_id", "status_code", "status_message",
		"trace_id",
	}
}

// spanKind names the kind for scripts, in the spanmetrics/OTLP spelling.
func spanKind(k ptrace.SpanKind) string {
	switch k {
	case ptrace.SpanKindInternal:
		return "internal"
	case ptrace.SpanKindServer:
		return "server"
	case ptrace.SpanKindClient:
		return "client"
	case ptrace.SpanKindProducer:
		return "producer"
	case ptrace.SpanKindConsumer:
		return "consumer"
	}
	return "unspecified"
}

func (s *spanObj) Attr(name string) (starlark.Value, error) {
	switch name {
	case "name":
		return starlark.String(s.sp.Name()), nil
	case "status_code":
		return starlark.MakeInt(int(s.sp.Status().Code())), nil
	case "attributes":
		return attrsView{s.sp.Attributes()}, nil
	case "resource":
		return attrsView{s.res.Attributes()}, nil
	case "drop":
		return dropFn{mark: func() { s.sp.Attributes().PutBool(DropMarker, true) }}, nil
	case "route":
		return routeFn(s.res), nil
	case "emit_metric":
		return emitFn(s.res, s.em), nil
	case "kind":
		return starlark.String(spanKind(s.sp.Kind())), nil
	case "status_message":
		return starlark.String(s.sp.Status().Message()), nil
	case "duration_ms":
		d := int64(s.sp.EndTimestamp()) - int64(s.sp.StartTimestamp())
		return starlark.Float(float64(d) / 1e6), nil
	case "trace_id":
		return hexID(s.sp.TraceID().String(), s.sp.TraceID().IsEmpty()), nil
	case "span_id":
		return hexID(s.sp.SpanID().String(), s.sp.SpanID().IsEmpty()), nil
	}
	return nil, nil
}

func (s *spanObj) SetField(name string, v starlark.Value) error {
	if name == "name" {
		str, ok := starlark.AsString(v)
		if !ok {
			return fmt.Errorf("name must be a string")
		}
		s.sp.SetName(str)
		return nil
	}
	return fmt.Errorf("cannot set %s", name)
}

// --- metric batch ---

type metricBatch struct {
	md pmetric.Metrics
	em MetricEmitter
}

func (b *metricBatch) String() string        { return "metric_batch" }
func (b *metricBatch) Type() string          { return "metric_batch" }
func (b *metricBatch) Freeze()               {}
func (b *metricBatch) Truth() starlark.Bool  { return true }
func (b *metricBatch) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

// Iterate walks the batch positionally — see logBatch.Iterate.
func (b *metricBatch) Iterate() starlark.Iterator {
	return &metricIter{rms: b.md.ResourceMetrics(), em: b.em}
}

type metricIter struct {
	rms     pmetric.ResourceMetricsSlice
	em      MetricEmitter
	i, j, k int
}

func (it *metricIter) Next(v *starlark.Value) bool {
	for it.i < it.rms.Len() {
		rm := it.rms.At(it.i)
		sms := rm.ScopeMetrics()
		if it.j >= sms.Len() {
			it.i, it.j, it.k = it.i+1, 0, 0
			continue
		}
		mets := sms.At(it.j).Metrics()
		if it.k >= mets.Len() {
			it.j, it.k = it.j+1, 0
			continue
		}
		*v = &metricObj{m: mets.At(it.k), res: rm.Resource(), em: it.em}
		it.k++
		return true
	}
	return false
}
func (it *metricIter) Done() {}

type metricObj struct {
	m   pmetric.Metric
	res pcommon.Resource
	em  MetricEmitter
}

func (m *metricObj) String() string        { return "metric" }
func (m *metricObj) Type() string          { return "metric" }
func (m *metricObj) Freeze()               {}
func (m *metricObj) Truth() starlark.Bool  { return true }
func (m *metricObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (m *metricObj) AttrNames() []string {
	return []string{"datapoints", "description", "drop", "emit_metric", "name", "resource", "route", "type", "unit"}
}

// metricType names the metric's data shape for scripts (`m.type == "sum"`).
func metricType(m pmetric.Metric) string {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return "gauge"
	case pmetric.MetricTypeSum:
		return "sum"
	case pmetric.MetricTypeHistogram:
		return "histogram"
	case pmetric.MetricTypeExponentialHistogram:
		return "exponential_histogram"
	case pmetric.MetricTypeSummary:
		return "summary"
	}
	return "empty"
}

func (m *metricObj) Attr(name string) (starlark.Value, error) {
	switch name {
	case "name":
		return starlark.String(m.m.Name()), nil
	case "type":
		return starlark.String(metricType(m.m)), nil
	case "unit":
		return starlark.String(m.m.Unit()), nil
	case "description":
		return starlark.String(m.m.Description()), nil
	case "resource":
		return attrsView{m.res.Attributes()}, nil
	case "datapoints":
		return &datapoints{m: m.m}, nil
	case "route":
		return routeFn(m.res), nil
	case "emit_metric":
		return emitFn(m.res, m.em), nil
	case "drop":
		// Mark in the pdata Metadata map, NOT the name — a script doing
		// `m.drop(); m.name = "x"` would otherwise overwrite a name-based
		// marker and silently un-drop the metric. (Metadata is a real OTLP
		// field, but a marked metric is pruned whole before export, so the
		// marker never reaches the wire.) Logs/spans mark in an attribute, so
		// drop() is already
		// order-independent for them; this makes it so for metrics too.
		return dropFn{mark: func() { m.m.Metadata().PutBool(DropMarker, true) }}, nil
	}
	return nil, nil
}

func (m *metricObj) SetField(name string, v starlark.Value) error {
	s, ok := starlark.AsString(v)
	if !ok {
		return fmt.Errorf("%s must be a string", name)
	}
	switch name {
	case "name":
		m.m.SetName(s)
	case "unit":
		m.m.SetUnit(s)
	case "description":
		m.m.SetDescription(s)
	default:
		return fmt.Errorf("cannot set %s", name)
	}
	return nil
}

// datapoints is the iterable view of a metric's data points, across all five
// pdata types. Without it a metrics script could only rename or drop a whole
// metric: the labels that actually drive cost and cardinality live on the data
// points, so "drop the pod_name label from this one noisy metric" — the common
// reason to reach for a metrics transform at all — was inexpressible.
type datapoints struct{ m pmetric.Metric }

func (d *datapoints) String() string        { return "datapoints" }
func (d *datapoints) Type() string          { return "datapoints" }
func (d *datapoints) Freeze()               {}
func (d *datapoints) Truth() starlark.Bool  { return d.Len() > 0 }
func (d *datapoints) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

// Len makes len(m.datapoints) work and lets a script skip empty metrics.
// One five-way switch, one owner: engine.go's dataPointCount.
func (d *datapoints) Len() int { return dataPointCount(d.m) }

// Iterate walks the metric's data points positionally — see logBatch.Iterate.
// A promscrape chunk is 10,000 points, and materializing them all cost
// 52,130 allocations before the script's first statement.
func (d *datapoints) Iterate() starlark.Iterator {
	return &dpIter{m: d.m}
}

type dpIter struct {
	m pmetric.Metric
	i int
}

func (it *dpIter) Next(v *starlark.Value) bool {
	if it.i >= dataPointCount(it.m) {
		return false
	}
	i := it.i
	it.i++
	switch it.m.Type() {
	case pmetric.MetricTypeGauge:
		p := it.m.Gauge().DataPoints().At(i)
		*v = &dpObj{attrs: p.Attributes(), num: p, scalar: true}
	case pmetric.MetricTypeSum:
		p := it.m.Sum().DataPoints().At(i)
		*v = &dpObj{attrs: p.Attributes(), num: p, scalar: true}
	case pmetric.MetricTypeHistogram:
		*v = &dpObj{attrs: it.m.Histogram().DataPoints().At(i).Attributes()}
	case pmetric.MetricTypeExponentialHistogram:
		*v = &dpObj{attrs: it.m.ExponentialHistogram().DataPoints().At(i).Attributes()}
	case pmetric.MetricTypeSummary:
		*v = &dpObj{attrs: it.m.Summary().DataPoints().At(i).Attributes()}
	default:
		return false
	}
	return true
}
func (it *dpIter) Done() {}

// dpObj is one data point: its attributes, its value where that is a single
// number, and drop(). scalar is false for the bucketed kinds, whose "value" is
// a distribution rather than a number; num is then the zero value and is never
// read.
type dpObj struct {
	attrs  pcommon.Map
	num    pmetric.NumberDataPoint
	scalar bool
}

func (p *dpObj) String() string        { return "datapoint" }
func (p *dpObj) Type() string          { return "datapoint" }
func (p *dpObj) Freeze()               {}
func (p *dpObj) Truth() starlark.Bool  { return true }
func (p *dpObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (p *dpObj) AttrNames() []string { return []string{"attributes", "drop", "value"} }

func (p *dpObj) Attr(name string) (starlark.Value, error) {
	switch name {
	case "attributes":
		return attrsView{p.attrs}, nil
	case "value":
		if !p.scalar {
			return starlark.None, nil // bucketed points have no scalar value
		}
		if p.num.ValueType() == pmetric.NumberDataPointValueTypeInt {
			return starlark.MakeInt64(p.num.IntValue()), nil
		}
		return starlark.Float(p.num.DoubleValue()), nil
	case "drop":
		// Marked in the point's own attributes and swept after the run, like
		// logs and spans. A metric left with no points at all is pruned too —
		// an empty metric is not a valid OTLP payload element to ship.
		return dropFn{mark: func() { p.attrs.PutBool(DropMarker, true) }}, nil
	}
	return nil, nil
}

func (p *dpObj) SetField(name string, v starlark.Value) error {
	if name != "value" {
		return fmt.Errorf("cannot set %s", name)
	}
	if !p.scalar {
		return fmt.Errorf("cannot set value on a %s data point", "bucketed")
	}
	switch x := v.(type) {
	case starlark.Int:
		i, ok := x.Int64()
		if !ok {
			return fmt.Errorf("value out of int64 range")
		}
		p.num.SetIntValue(i)
	case starlark.Float:
		p.num.SetDoubleValue(float64(x))
	default:
		return fmt.Errorf("value must be a number")
	}
	return nil
}

// routeFn is the route("name") verb on every item: it stamps the item's
// RESOURCE with the reserved attribute the namespace router honors before
// its globs (route.ScriptMarker, stripped by the router before anything is
// sent). Scripts could already steer routing by rewriting
// k8s.namespace.name; this is the sanctioned spelling. Resource-scoped by
// nature: routing splits payloads per resource, so routing one record
// routes its whole resource group.
func routeFn(res pcommon.Resource) starlark.Value {
	return starlark.NewBuiltin("route", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var name string
		if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &name); err != nil {
			return nil, err
		}
		res.Attributes().PutStr(route.ScriptMarker, name)
		return starlark.None, nil
	})
}

// emitFn is the emit_metric(name, value, labels={}) verb: one observation
// into a metric DECLARED in the logMetrics config (declaration is where the
// type, action, buckets and cardinality cap live), grouped under this item's
// resource. An undeclared name — or no logMetrics section at all — is a
// script error, surfaced like any other (obs.TransformErrors + the export's
// retry); the fix is a config edit, and both files hot-reload.
func emitFn(res pcommon.Resource, em MetricEmitter) starlark.Value {
	return starlark.NewBuiltin("emit_metric", func(_ *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var name string
		var value starlark.Value
		var lblDict *starlark.Dict
		if err := starlark.UnpackArgs(b.Name(), args, kwargs, "name", &name, "value", &value, "labels?", &lblDict); err != nil {
			return nil, err
		}
		var f float64
		switch x := value.(type) {
		case starlark.Int:
			i, ok := x.Int64()
			if !ok {
				return nil, fmt.Errorf("emit_metric: value out of range")
			}
			f = float64(i)
		case starlark.Float:
			f = float64(x)
		default:
			return nil, fmt.Errorf("emit_metric: value must be a number")
		}
		var lbls map[string]string
		if lblDict != nil {
			lbls = make(map[string]string, lblDict.Len())
			for _, kv := range lblDict.Items() {
				k, ok := starlark.AsString(kv[0])
				if !ok {
					return nil, fmt.Errorf("emit_metric: label keys must be strings")
				}
				v, ok := starlark.AsString(kv[1])
				if !ok {
					v = kv[1].String()
				}
				lbls[k] = v
			}
		}
		if em == nil {
			return nil, fmt.Errorf("emit_metric %q: no logMetrics section is configured", name)
		}
		if err := em.EmitDirect(name, f, lbls, res.Attributes()); err != nil {
			return nil, err
		}
		return starlark.None, nil
	})
}
