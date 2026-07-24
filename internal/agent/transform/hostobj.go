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

// dropMarker flags an element for post-run pruning. Metrics have no
// attributes at the metric level, so a dropped metric's NAME is set to it
// instead — either way the marker never survives into the export.
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

func (b *logBatch) Iterate() starlark.Iterator {
	var recs []*logRecord
	rls := b.ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				recs = append(recs, &logRecord{lr: lrs.At(k), res: rl.Resource()})
			}
		}
	}
	return &sliceIter[*logRecord]{items: recs}
}

type sliceIter[T starlark.Value] struct {
	items []T
	i     int
}

func (it *sliceIter[T]) Next(v *starlark.Value) bool {
	if it.i >= len(it.items) {
		return false
	}
	*v = it.items[it.i]
	it.i++
	return true
}
func (it *sliceIter[T]) Done() {}

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

func (b *traceBatch) Iterate() starlark.Iterator {
	var spans []*spanObj
	rss := b.td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				spans = append(spans, &spanObj{sp: sps.At(k), res: rs.Resource()})
			}
		}
	}
	return &sliceIter[*spanObj]{items: spans}
}

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

func (b *metricBatch) Iterate() starlark.Iterator {
	var ms []*metricObj
	rms := b.md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			mets := sms.At(j).Metrics()
			for k := 0; k < mets.Len(); k++ {
				ms = append(ms, &metricObj{m: mets.At(k), res: rm.Resource()})
			}
		}
	}
	return &sliceIter[*metricObj]{items: ms}
}

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
	return []string{"drop", "name", "resource"}
}

func (m *metricObj) Attr(name string) (starlark.Value, error) {
	switch name {
	case "name":
		return starlark.String(m.m.Name()), nil
	case "resource":
		return attrsView{m.res.Attributes()}, nil
	case "drop":
		return dropFn{mark: func() { m.m.SetName(dropMarker) }}, nil
	}
	return nil, nil
}

func (m *metricObj) SetField(name string, v starlark.Value) error {
	if name == "name" {
		s, ok := starlark.AsString(v)
		if !ok {
			return fmt.Errorf("name must be a string")
		}
		m.m.SetName(s)
		return nil
	}
	return fmt.Errorf("cannot set %s", name)
}
