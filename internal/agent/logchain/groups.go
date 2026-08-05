package logchain

// Flush-time grouping for the COLD producers (journald, events, azurediag):
// the string group key and the map + create-on-miss scaffold each of them
// carried — journald as a scopeEntry map, events and azurediag as two
// parallel maps (ScopeLogs and resource attrs keyed separately).
//
// The tailer deliberately keeps its own grouper: its per-line path is
// bench-pinned at zero allocations, so it keys on a STRUCT
// (scopeKey{*file, res, scope}) rather than a concatenated string, and its
// log-metrics bindKey EXCLUDES the scope half — scope attrs land on the
// scope, never the resource, so they cannot split a metric's identity
// (internal/agent/tailer/flush.go). Nothing here is for the tailer.

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// GroupKey is one record's grouping key: base alone without a configured
// extractor (no per-record concatenation), else base plus the line's lifted
// resource and scope attribute fingerprints — line-derived resource/scope
// attributes split records into their own ResourceLogs/ScopeLogs, so they
// must participate in the key.
func (c *Chain[K]) GroupKey(base string, extracted logattrs.Result) string {
	if c.cfg.LogAttrs == nil {
		return base
	}
	return base + "\x01" + logattrs.Key(extracted.Resource) + "\x01" + logattrs.Key(extracted.Scope)
}

// Group is one grouped destination: the scope a kept record lands in, plus
// the resource attributes the metrics and rules resolvers read (which plog
// does not expose back from a ScopeLogs).
type Group struct {
	SL  plog.ScopeLogs
	Res pcommon.Map
}

// ResourceFiller fills a fresh group's RESOURCE — the half that is genuinely
// per-producer (a systemd unit, an involved object, an ARM resource). It is
// an interface, not a closure parameter, for the chain's Producer reason: the
// producers' record sinks already carry the per-record state, and a func
// literal capturing loop variables would allocate per RECORD, hit or miss.
type ResourceFiller interface {
	FillResource(pcommon.Resource)
}

// Groups is one flush's group map.
type Groups struct {
	ld    plog.Logs
	name  string
	byKey map[string]Group
}

// NewGroups prepares grouping into ld; scopeName names every created scope.
func NewGroups(ld plog.Logs, scopeName string, hint int) Groups {
	return Groups{ld: ld, name: scopeName, byKey: make(map[string]Group, hint)}
}

// Get returns key's group, creating it on first sight: fill builds the
// resource (identity attributes first, so templates and the filter see them),
// then the line-lifted resource attributes go on, then the named scope with
// its lifted scope attributes. Callers create the group BEFORE Emit because
// metric and rule resolution reads the group's own resource; a group the
// rules empty is pruned afterwards (Prune) rather than never created.
func (g *Groups) Get(key string, extracted logattrs.Result, fill ResourceFiller) Group {
	ent, ok := g.byKey[key]
	if !ok {
		rl := g.ld.ResourceLogs().AppendEmpty()
		res := rl.Resource()
		fill.FillResource(res)
		logattrs.Put(res.Attributes(), extracted.Resource)
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(g.name)
		sl.Scope().SetVersion(obs.ScopeVersion)
		logattrs.Put(sl.Scope().Attributes(), extracted.Scope)
		ent = Group{SL: sl, Res: res.Attributes()}
		g.byKey[key] = ent
	}
	return ent
}
