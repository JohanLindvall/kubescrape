package cgroupstats

// The export path: rendering each window into its distribution (snapshot,
// finish), retiring the containers whose windows are final, rebuilding every
// identity and sending one payload per interval.

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// stats is the distribution one window of one signal measured: the four
// statistics every export carries per signal beside the sample count. It is
// what a signal's rendering (signalOut) and its sparse-window hold (held) have
// in common, so it is spelled once.
type stats struct{ stddev, max, min, mean float64 }

// stats renders the window's distribution. Meaningful from two samples up; see
// measured.
func (w *window) stats() stats {
	return stats{stddev: w.stddev(), max: w.max, min: w.min, mean: w.mean}
}

// held is the last distribution exported for one signal of one container, and
// how many consecutive windows have now re-stated it (bounded by
// maxHeldWindows).
type held struct {
	ok bool
	n  int
	stats
}

// signalOut is one signal's rendered contribution to one export.
//
// samples is THIS window's own sample count, and it is what makes every other
// number in the struct readable: below two the four statistics beside it are
// the PREVIOUS window's, re-stated (see finish), and between two and thirty
// they say how much of a distribution the window actually measured. It is
// deliberately not taken from the held value — a re-statement that also
// re-stated its count would be indistinguishable from a fresh measurement,
// which is the whole reason it is exported.
type signalOut struct {
	emit bool
	stats
	samples int64
}

// values is the signal's five gauge values in the order of the gauge tables
// (cpuGauges, memGauges), which is what putSignal pairs them by.
func (o signalOut) values() [gaugesPerSignal]float64 {
	return [gaugesPerSignal]float64{o.stddev, o.max, o.min, o.mean, float64(o.samples)}
}

// windowPair is one container's finished window, copied out from under the
// lock so the identity rebuild and the OTLP build can run without it.
//
// It carries the two IDS rather than a resource: the resource is rebuilt from
// current metadata at every export (see the package doc), so there is nothing
// resource-shaped to copy out, and the ids are exactly what the resolver takes.
type windowPair struct {
	id     string
	podUID string
	cpu    signalOut
	mem    signalOut
}

// exportLoop ships one window per tick. It returns on cancellation WITHOUT
// exporting; the final window is FinalExport's, after the sampler has stopped,
// so it cannot race a sweep that is still writing into it.
func (s *Sampler) exportLoop(ctx context.Context, exp Exporter, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	// Failure state is LOCAL: this goroutine is the loop's only exporter
	// narrating anything (FinalExport's caller reports its own outcome).
	var narr exportNarration
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.exportTick(ctx, exp, &narr)
		}
	}
}

// exportTick is one tick of exportLoop: ship the window and narrate the
// outcome. Separate from the loop so a test can drive ticks without a timer.
func (s *Sampler) exportTick(ctx context.Context, exp Exporter, narr *exportNarration) {
	sent, err := s.exportWindow(ctx, exp)
	narr.note(s.log, sent, err)
}

// exportReWarnEvery restates a failing export while the failure persists —
// internal/metrics' reWarnInterval, for the same reason. readWarnEvery's
// minute would only halve the lines at the default 30s -scrape-interval.
const exportReWarnEvery = 5 * time.Minute

// exportNarration reports the export loop's outcomes as TRANSITIONS rather than
// once per tick, the shape internal/metrics.Registry.Run and
// DynamicMetricSet.noteExport already take.
//
// This used to be one unthrottled Warn per failed export: at the default 30s
// -scrape-interval, two identical lines a minute per node for as long as the
// collector was down — on top of otlpexport's own transition narration — and
// no counterpart, so an operator watching them stop could not tell a recovery
// from a sampler that had stopped exporting at all. A collector outage is a
// persisting STATE, which is what internal/logdedupe is for. So the first
// failure of a run warns, the repeats restate themselves every
// exportReWarnEvery carrying the cost so far (Debug in between), and the first
// export that SENDS something afterwards says it recovered.
//
// Unlike the self-metrics Registry, whose samples survive a failed push, a
// failed window here is GONE — snapshot reset it as it rendered — so the Warn
// says so and names the counter carrying the rate; the line is the narration,
// kubescrape_cgroup_windows_dropped_total{reason="export_failed"} the loss.
type exportNarration struct {
	outage logdedupe.Outage
}

// note records one tick. sent is whether a payload was offered to the exporter
// at all: an export with nothing to send says nothing about the collector, so
// it neither extends an outage nor ends one.
func (n *exportNarration) note(log *slog.Logger, sent bool, err error) {
	switch {
	case err != nil:
		now := time.Now()
		if _, loud := n.outage.Fail(now, exportReWarnEvery); loud {
			log.Warn("cgroup-stats export failed; each failed window is lost, not re-offered, and counted in "+
				`kubescrape_cgroup_windows_dropped_total{reason="export_failed"}`,
				"error", err, "failures", n.outage.Failures(), "outage", n.outage.Lasted(now))
			return
		}
		log.Debug("cgroup-stats export failed", "error", err, "failures", n.outage.Failures())
	case sent && n.outage.Failing():
		failures, lasted, _ := n.outage.Recover(time.Now())
		log.Info("cgroup-stats export recovered", "failures", failures, "outage", lasted)
	}
}

// snapshot renders every window and resets it, under the lock. The copy is what
// lets the identity rebuild and the OTLP build run with the lock free.
//
// The windows are reset HERE rather than after a successful send: they describe
// one bounded interval, so carrying a failed window forward would either
// double-count it into the next one or silently stretch the interval the
// numbers claim to cover. A lost window is one gap in a distribution; a wrong
// one is worse. With -buffer-dir the send is an enqueue, so the outage case
// this trades away is already covered by the spool. What the reset is NOT
// allowed to be is silent: every window discarded — by a failed send, or by an
// identity that could not be rebuilt — is counted into obs.CgroupWindowsDropped
// by the caller, since a loss nobody can see is the one thing worse than a loss.
//
// It is also where containers are RETIRED, in the two ways one can be: a GONE
// container (its cgroup left the hierarchy) ships its accumulated window once
// (finalWindowLocked) and is dropped, and one that is still listed but has
// answered no read for maxDeadWindows windows is dropped too
// (retireUnreadableLocked).
//
// The caller holds exportSem, which is what makes reusing s.snap safe.
func (s *Sampler) snapshot() []windowPair {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = s.snap[:0]
	for id, c := range s.tracked {
		var cpu, mem signalOut
		if c.gone {
			cpu, mem = s.finalWindowLocked(c)
		} else {
			cpu = s.finish(&c.cpu, &c.heldCPU)
			mem = s.finish(&c.mem, &c.heldMem)
		}
		if cpu.emit || mem.emit {
			s.snap = append(s.snap, windowPair{id: c.id, podUID: c.podUID, cpu: cpu, mem: mem})
			// This container has now been described at least once, which is
			// what finalWindowLocked's too_short verdict turns on.
			c.described = true
		}
		c.cpu.reset()
		c.mem.reset()
		switch {
		case c.gone:
			c.release()
			delete(s.tracked, id)
		case c.endWindow():
			s.retireUnreadableLocked(id, c)
		}
	}
	s.publishCountsLocked()
	return s.snap
}

// finalWindowLocked renders a GONE container's last window and counts it when
// no window of the container's life could be described. Caller holds mu.
//
// A dead container's LAST datapoint is the one an operator zooms into after an
// OOM kill, so it has to be real. finish would hold here — re-emitting the
// PREVIOUS window's numbers stamped at the retirement instant — and the hold
// exists to bridge a sampling hiccup in a LIVING container, not to invent a
// final reading for a container that has stopped producing them. A gap is the
// honest report of a window that measured too little to describe.
//
// The literal test: this container was resolved and had three descriptors open,
// and its cgroup left the hierarchy without either signal ever yielding two
// readings — so no window of its life can be described. Everything measured
// about it is discarded here, so it is counted:
// obs.CgroupWindowsDropped{reason="too_short"}.
//
// THE NAME IS THE COMMON CASE, NOT THE WHOLE BRANCH. Two neighbours land here
// as well. Both are genuine losses of a window this node can say nothing about,
// so the lower-bound property survives — but neither is a short-lived
// container, and an operator reading a rate off this series should know which
// of the three they might be looking at:
//
//   - a container a pass discovered whose cgroup was already gone by the time
//     the sweep reached it. It held three descriptors and yielded no reading at
//     all — every attempt failing, or none attempted if the next pass marked it
//     gone first. "Too short to measure" is right about the outcome and wrong
//     about the cause: what was late was the discovery, not short the life.
//   - a container that lived for MINUTES whose three files failed every read —
//     a CRI-O container whose own scope was removed while its conmon scope
//     lingers, a stale listing; see maxDeadWindows — and whose cgroup then
//     vanished before the dead-window streak could retire it. That is an
//     unreadable-cgroup fault, and it is ALREADY counted, three times per
//     sampling period, as kubescrape_cgroup_read_errors_total. Read the two
//     together before concluding that a node is losing short-lived containers:
//     a rate here with a matching read error rate is the second case, not the
//     first.
//
// Three guards, and each one is what keeps the count from meaning something
// else:
//
//   - `described` excludes the container that has been exporting all along and
//     dies shortly after an export. Its final partial window is lost too, but
//     "never describable" is not what happened, and at a 1s period inside a 30s
//     window a few percent of every ORDINARY termination lands there. A signal
//     that fires on normal shutdowns is one nobody can alert on.
//   - `atShutdown` excludes THIS PROCESS exiting (see stop): every container is
//     gone by then, and a SIGTERM before the second sweep would otherwise
//     publish the verdict for the whole node.
//   - the emit tests are the event itself: a container that produced a
//     distribution of either signal is exported (snapshot appends its window)
//     and is not a loss at all.
//
// WHAT IT DOES NOT COVER, because the sampler has no evidence of it (stated
// here because the counter's value as an argument for a shorter
// -cgroup-stats-discover-interval turns on how far below the truth it sits —
// see DefaultDiscoverInterval):
//
//   - a cgroup that appeared and vanished BETWEEN two discovery passes. Nothing
//     in the hierarchy remembers it.
//   - a cgroup discovered but never RESOLVED — it lives in s.pending, holds no
//     descriptors, and reconcile simply deletes it. That set is dominated by
//     every pod's sandbox cgroup, which never resolves by construction, so
//     counting its deletions here would drown the signal in one increment per
//     pod terminated.
//   - a container discovered LATE in its life: it is sampled for the remainder,
//     and two seconds of remainder is enough to describe it.
//
// So the counter is a strict lower bound and a narrow one. It is worth having
// anyway because it is CERTAIN — every increment is a container this node
// measured and threw away — where the rest of the blind spot is unmeasurable.
func (s *Sampler) finalWindowLocked(c *container) (cpu, mem signalOut) {
	cpu, mem = measured(&c.cpu), measured(&c.mem)
	if !cpu.emit && !mem.emit && !c.described && !c.atShutdown {
		s.c.droppedTooShort.Inc()
	}
	return cpu, mem
}

// retireUnreadableLocked drops a container that is still listed but has
// answered no read for maxDeadWindows windows: released, dropped and counted,
// then quarantined on the slow clock. See maxDeadWindows. Caller holds mu.
func (s *Sampler) retireUnreadableLocked(id string, c *container) {
	c.release()
	delete(s.tracked, id)
	obs.CgroupContainersRetired.Inc()
	s.quarantineLocked(c)
	if s.retireWarn.Allow(readWarnEvery) {
		s.log.Warn("cgroup sampler retired a container that is still listed but has answered no read for several export windows (throttled); its descriptors are released and it is no longer counted as sampled",
			"dir", c.dir, "id", c.id, "windows", c.deadWindows)
	}
}

// quarantineLocked puts a retired container back into the PENDING set on the
// slow clock. Caller holds mu.
//
// Retirement alone would not stick: the cgroup is still in the hierarchy, so the
// very next discovery pass — fifteen seconds later — finds an id that is in
// neither map, resolves it (its identity is fine; it is the cgroup FILES that
// stopped answering), re-opens three descriptors and starts the same three dead
// windows over. That is a retirement undone before it saved anything: measured
// against the fifteen-second discovery cadence and a thirty-second window, such
// a container would be back to holding descriptors and failing reads for about
// five sixths of the time.
//
// The pending set is already exactly the right place — "discovery knows about
// it, nothing is being sampled from it" — and gaveUp is already how that set
// spells "ask about this one rarely". So the entry lands there past its grace
// period, and abandonRetryEvery later it gets one more chance: if whatever broke
// has healed it is sampled again, and if not it costs three failing reads for
// three windows out of every ten minutes instead of forever.
//
// One nuance worth stating rather than hiding: it now counts in
// kubescrape_cgroup_unresolved_containers, whose name is about IDENTITY, and
// this container's identity resolved perfectly well. The gauge's real subject is
// "discovered and not exported", which is true of it, and the retirement counter
// plus the warning are what name the actual reason.
// The pending CAP is deliberately not consulted: this entry is not a new
// discovery competing for memory, it is a container that already held three
// descriptors, and refusing it would put it straight back into the churn the
// quarantine exists to stop. The overshoot is bounded by the tracked cap
// (maxContainers entries of ~150 bytes) and only in a hierarchy that is already
// at both caps at once.
func (s *Sampler) quarantineLocked(c *container) {
	now := s.now()
	s.pending[c.id] = &pendingContainer{
		cgroupRef: c.cgroupRef,
		firstFail: now, lastTry: now, nextTry: now.Add(abandonRetryEvery), gaveUp: true,
		unreadable: true,
	}
}

// measured renders one signal's window WITHOUT the sparse-window hold: a real
// distribution or nothing. It is what a GONE container's final window gets
// (finalWindowLocked), and what finish renders a full window with.
func measured(w *window) signalOut {
	if w.n < 2 {
		return signalOut{}
	}
	return signalOut{emit: true, stats: w.stats(), samples: int64(w.n)}
}

// finish renders one signal's window and folds the sparse-window rule in.
//
// A window holding FEWER THAN TWO samples of a signal is not a distribution: a
// single reading has a stddev of zero by construction and a max and a min that
// are the same number — which is the average the cadvisor scrape already
// publishes, dressed up as three new series. So such a window HOLDS: the last
// distribution actually measured is re-emitted, and the series keeps its shape
// instead of alternating between real numbers and degenerate ones.
//
// Per SIGNAL, because the two fill at different rates: a CPU rate needs two raw
// readings before it yields even one value, so CPU is permanently one sample
// behind memory and a per-container rule would throw away a perfectly good
// working-set measurement to protect the CPU one.
//
// With nothing held — a container's very first window — nothing is emitted. A
// gap is honest about a distribution that has not been measured yet; inventing
// one is not. The hold is BOUNDED for the same reason (maxHeldWindows): past
// the bound the signal stops being emitted, because re-stating a measurement
// indefinitely is not bridging a gap, it is reporting a dead container as a
// live one. What happens to such a container one step later is maxDeadWindows'
// business: it is retired, so it stops costing descriptors and reads as well as
// series.
//
// A GONE container's final window does not come through here at all —
// finalWindowLocked renders it with measured alone. The hold bridges a hiccup in a LIVING container; a dead
// one's last datapoint has to be something that was actually read.
//
// A held window carries NO MARKER distinguishing it downstream, and that is a
// decision rather than an omission. The only two places a marker could go both
// fork the series: a resource attribute changes the derived job and instance,
// which is the exact identity flap the whole design is built to prevent, and a
// data-point attribute on a gauge changes the label set, which breaks the join
// to the cadvisor series these gauges exist to annotate — for precisely the
// windows it marks. So the bound is the answer instead: at most maxHeldWindows
// re-statements, then a gap, and the gap is the signal. What an operator
// staring at a flat max needs is on the AGENT's own metrics, where it costs no
// series identity: obs.CgroupHeldWindows{outcome} counts both the holding and
// the giving up.
func (s *Sampler) finish(w *window, h *held) signalOut {
	if out := measured(w); out.emit {
		*h = held{ok: true, stats: out.stats}
		return out
	}
	if !h.ok {
		return signalOut{}
	}
	if h.n >= maxHeldWindows {
		// Cleared, not merely skipped: this is what makes `expired` an EVENT
		// counted once at the transition rather than once per window forever,
		// and what makes a later real window start a fresh hold budget.
		*h = held{}
		s.c.heldExpired.Inc()
		return signalOut{}
	}
	h.n++
	s.c.heldWindows.Inc()
	// samples is w.n — 0 or 1 — and NOT the held value's: it is the one field
	// that must describe this window rather than the one being re-stated, so
	// that a consumer can tell the two apart at all. See signalOut.
	return signalOut{emit: true, stats: h.stats, samples: int64(w.n)}
}

// export snapshots, rebuilds every identity and sends one window.
//
// The whole body is serialised by exportSem: the export loop and FinalExport
// are concurrent in production (see the field), and two of these interleaving
// would race on the snapshot scratch and split one window's containers across
// two payloads.
func (s *Sampler) export(ctx context.Context, exp Exporter) error {
	_, err := s.exportWindow(ctx, exp)
	return err
}

// exportWindow is export, also reporting whether a payload was offered to the
// exporter at all (an empty window sends nothing, which says nothing about the
// collector; see exportNarration.note).
func (s *Sampler) exportWindow(ctx context.Context, exp Exporter) (sent bool, err error) {
	select {
	case s.exportSem <- struct{}{}:
	case <-ctx.Done():
		// The other exporter has the window and is shipping it; waiting past
		// the caller's deadline would spend a shutdown budget the later steps
		// need. Nothing is dropped here that the holder is not already sending.
		return false, ctx.Err()
	}
	defer func() { <-s.exportSem }()

	snap := s.snapshot()
	if len(snap) == 0 {
		return false, nil
	}
	md := s.build(ctx, snap, s.now())
	if md.DataPointCount() == 0 {
		return false, nil
	}
	if err := exp.ExportMetrics(ctx, md); err != nil {
		s.c.droppedExport.Add(float64(md.ResourceMetrics().Len()))
		return true, err
	}
	return true, nil
}

// FinalExport ships the last window, on the CALLER's context.
//
// The caller owns the budget deliberately: this used to manufacture its own
// five seconds, which is the shape internal/metrics.Registry.FinalExport had
// removed from it — a fixed timeout cannot be fitted inside the pod's
// termination grace, which the agent's shutdown sequence is tracking as one
// shared deadline for every final flush. The context must also be DETACHED
// (WithoutCancel, never a bare Background: otlpexport.Own's durability marker
// and the transform handoff marker ride on it).
//
// It is safe to call while Run is still going, and the agent does exactly that:
// the shutdown sequence joins its producers under a bounded budget and proceeds
// when the join times out. exportSem serialises the two, and the ctx bounds the
// wait. Once Run HAS returned every descriptor is released and every container
// is marked for a final flush, so this then carries whatever the last partial
// window measured, including the seconds of a container that vanished just
// before shutdown.
func (s *Sampler) FinalExport(ctx context.Context, exp Exporter) error {
	return s.export(ctx, exp)
}

// build rebuilds each container's identity and renders one ResourceMetrics per
// container that still has one, with up to ten gauges (five per signal, and a
// signal whose window measured nothing contributes none of its five).
//
// The rebuild runs HERE, on the export goroutine with the mutex free, and never
// on the sampler goroutine: an identity lookup can block for as long as the
// metadata service takes, and the sweep must keep measuring the node while it
// does. See resolveExport for the budget.
func (s *Sampler) build(ctx context.Context, snap []windowPair, now time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	ts := pcommon.NewTimestampFromTime(now)
	rctx, cancel := context.WithTimeout(ctx, s.exportBudget)
	defer cancel()
	for i := range snap {
		w := &snap[i]
		res := pcommon.NewResource()
		if !s.resolveExport(rctx, res, w) {
			continue
		}
		rm := md.ResourceMetrics().AppendEmpty()
		res.MoveTo(rm.Resource())
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(scopeName)
		sm.Scope().SetVersion(obs.ScopeVersion)
		putSignal(sm, &cpuGauges, w.cpu, ts)
		putSignal(sm, &memGauges, w.mem, ts)
	}
	return md
}

// resolveExport rebuilds one container's resource for one export, reporting
// whether it may be exported at all.
//
// The cost is one Resolver call per container per export, and the claim that
// this is cheap is load-bearing enough to be measured rather than assumed
// (promscrape's TestFillContainerResourceIsA304InTheSteadyState): the resolver
// keeps a one-minute cache keyed by container id — the SAME entries the
// cadvisor scrape fills, since it resolves the same ids through the same body —
// and a miss underneath it reaches metaclient, which holds the decoded
// document with its ETag and revalidates a stale one as a conditional GET the
// metadata service answers 304. So the steady state is a map lookup, and the
// worst case once a minute per container is a 304 with no body.
//
// A container that does not resolve is NOT exported and its window is LOST —
// counted, because it was measured. That is the same verdict discovery applies
// before spending descriptors, taken again here because a container's identity
// can stop resolving between the two (a metadata-service outage, a pod whose
// tombstone aged out from under a cgroup that has not been cleaned up yet).
//
// A lookup cut short by the pass deadline lands here too, which is the right
// accounting for the LOSS counter (the window really is gone) and a slightly
// generous reading of the unresolved one — but only in the state where the
// metadata service is too slow to answer a node's worth of lookups, which is
// exactly what that counter is there to show.
// The failure CLASSIFICATION is deliberately ignored here. It governs a retry
// cadence for a cgroup that is not being sampled, and everything reaching this
// seam is already tracked and will be offered again by the next export whatever
// the answer was; the pending set's policy is resolveFailed's.
func (s *Sampler) resolveExport(ctx context.Context, res pcommon.Resource, w *windowPair) bool {
	ok, _ := s.resolver.FillContainerResource(ctx, res, w.id, w.podUID)
	if !ok {
		s.c.droppedUnresolved.Inc()
		s.c.unresolvedExport.Inc()
		return false
	}
	return true
}

// putSignal renders one signal's five gauges — or none, for a signal whose
// window measured nothing — pairing each row of its gauge table with the value
// at the same position (signalOut.values). The table IS the emitted set, so a
// gauge cannot be added to the wire without being added to it.
func putSignal(sm pmetric.ScopeMetrics, specs *[gaugesPerSignal]gaugeSpec, o signalOut, ts pcommon.Timestamp) {
	if !o.emit {
		return
	}
	vals := o.values()
	for i := range specs {
		putGauge(sm, &specs[i], vals[i], ts)
	}
}

func putGauge(sm pmetric.ScopeMetrics, g *gaugeSpec, v float64, ts pcommon.Timestamp) {
	m := sm.Metrics().AppendEmpty()
	m.SetName(g.name)
	m.SetDescription(g.desc)
	if g.unit != "" {
		m.SetUnit(g.unit)
	}
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(v)
}
