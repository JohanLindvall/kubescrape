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
)

// dropMarker flags an element for post-run pruning. Logs and spans carry it
// as a record/span attribute; metrics carry it in the pdata Metadata map,
// which keeps it OFF the data-point attributes a rename could touch, so a
// drop-then-rename cannot un-drop it. The marker never survives into the
// export regardless — a marked metric is pruned whole and a script error
// discards the transform's whole working copy — so it never reaches the wire
// (the Metadata field IS an OTLP wire field, unlike an earlier claim here).
const dropMarker = "__kubescrape_drop__"

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
	return fromStarlark(a.m.PutEmpty(key), v)
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

type logBatch struct{ ld plog.Logs }

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
	return &logIter{rls: b.ld.ResourceLogs()}
}

type logIter struct {
	rls     plog.ResourceLogsSlice
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
		*v = &logRecord{lr: lrs.At(it.k), res: rl.Resource()}
		it.k++
		return true
	}
	return false
}
func (it *logIter) Done() {}

// logRecord exposes body, severity_text, severity_number, attributes,
// resource and drop().
type logRecord struct {
	lr  plog.LogRecord
	res pcommon.Resource
}

func (r *logRecord) String() string        { return "log_record" }
func (r *logRecord) Type() string          { return "log_record" }
func (r *logRecord) Freeze()               {}
func (r *logRecord) Truth() starlark.Bool  { return true }
func (r *logRecord) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (r *logRecord) AttrNames() []string {
	return []string{"attributes", "body", "drop", "resource", "severity_number", "severity_text"}
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
		return dropFn{mark: func() { r.lr.Attributes().PutBool(dropMarker, true) }}, nil
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
	}
	return fmt.Errorf("cannot set %s", name)
}

// --- trace batch ---

type traceBatch struct{ td ptrace.Traces }

func (b *traceBatch) String() string        { return "trace_batch" }
func (b *traceBatch) Type() string          { return "trace_batch" }
func (b *traceBatch) Freeze()               {}
func (b *traceBatch) Truth() starlark.Bool  { return true }
func (b *traceBatch) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

// Iterate walks the batch positionally — see logBatch.Iterate.
func (b *traceBatch) Iterate() starlark.Iterator {
	return &spanIter{rss: b.td.ResourceSpans()}
}

type spanIter struct {
	rss     ptrace.ResourceSpansSlice
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
		*v = &spanObj{sp: sps.At(it.k), res: rs.Resource()}
		it.k++
		return true
	}
	return false
}
func (it *spanIter) Done() {}

type spanObj struct {
	sp  ptrace.Span
	res pcommon.Resource
}

func (s *spanObj) String() string        { return "span" }
func (s *spanObj) Type() string          { return "span" }
func (s *spanObj) Freeze()               {}
func (s *spanObj) Truth() starlark.Bool  { return true }
func (s *spanObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (s *spanObj) AttrNames() []string {
	return []string{"attributes", "drop", "name", "resource", "status_code"}
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
		return dropFn{mark: func() { s.sp.Attributes().PutBool(dropMarker, true) }}, nil
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

type metricBatch struct{ md pmetric.Metrics }

func (b *metricBatch) String() string        { return "metric_batch" }
func (b *metricBatch) Type() string          { return "metric_batch" }
func (b *metricBatch) Freeze()               {}
func (b *metricBatch) Truth() starlark.Bool  { return true }
func (b *metricBatch) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

// Iterate walks the batch positionally — see logBatch.Iterate.
func (b *metricBatch) Iterate() starlark.Iterator {
	return &metricIter{rms: b.md.ResourceMetrics()}
}

type metricIter struct {
	rms     pmetric.ResourceMetricsSlice
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
		*v = &metricObj{m: mets.At(it.k), res: rm.Resource()}
		it.k++
		return true
	}
	return false
}
func (it *metricIter) Done() {}

type metricObj struct {
	m   pmetric.Metric
	res pcommon.Resource
}

func (m *metricObj) String() string        { return "metric" }
func (m *metricObj) Type() string          { return "metric" }
func (m *metricObj) Freeze()               {}
func (m *metricObj) Truth() starlark.Bool  { return true }
func (m *metricObj) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable") }

func (m *metricObj) AttrNames() []string {
	return []string{"datapoints", "description", "drop", "name", "resource", "type", "unit"}
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
	case "drop":
		// Mark in the pdata Metadata map, NOT the name — a script doing
		// `m.drop(); m.name = "x"` would otherwise overwrite a name-based
		// marker and silently un-drop the metric. (Metadata is a real OTLP
		// field, but a marked metric is pruned whole before export, so the
		// marker never reaches the wire.) Logs/spans mark in an attribute, so
		// drop() is already
		// order-independent for them; this makes it so for metrics too.
		return dropFn{mark: func() { m.m.Metadata().PutBool(dropMarker, true) }}, nil
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
		return dropFn{mark: func() { p.attrs.PutBool(dropMarker, true) }}, nil
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
