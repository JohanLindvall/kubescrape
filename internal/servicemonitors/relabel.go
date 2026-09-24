package servicemonitors

// The metricRelabelings walk (relabelChain): which keep/drop rules are applied,
// the ceilings on the chain, the report of what was refused, and the tracking
// of unapplied label-mutating rules a later filter may depend on.

import (
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Bounds on the metricRelabelings chain ONE monitor endpoint may impose. They
// exist for the reason the tailer bounds a pod's log rules
// (internal/agent/tailer/podconfig.go, maxPodRules): the chain is
// TENANT-SUPPLIED — anyone with namespace edit rights can create a
// ServiceMonitor, and with -monitor-namespaces unset (the default, "all
// namespaces honoured") a `selector: {}` + `namespaceSelector.any: true`
// monitor attaches to every Service in the cluster — and its cost is paid by
// somebody else, twice over and linearly in its size:
//
//   - BYTES. scrape.stampEndpoint copies the whole chain into EVERY
//     kubemeta.ScrapeTarget the monitor resolves to, and
//     /v1/nodes/{node}/targets marshals every target of every pod on the node
//     into one []byte. A 1.14 MiB chain — ~20k rules, comfortably inside
//     etcd's object limit — turns a ~2 MiB node document into a multi-GiB
//     allocation, in a singleton whose chart requests 128Mi and sets no limit.
//     scrape.MaxPortsPerPod does not see this: it bounds the target COUNT
//     against a per-target cost it models as the pod document (~2 KiB), which
//     is exactly the lesson recorded there as "a bound on ENTRIES is not a
//     bound on BYTES".
//   - CPU, on every agent that scrapes such a target: the agent's
//     relabelFilter.Keep walks EVERY rule for EVERY sample with no memo (unlike
//     its sibling filterSession), for the whole scrape timeout, every cycle.
//
// The numbers are far above any legitimate chain: kube-prometheus-stack's own
// monitors carry a handful of rules each, and a metric ALLOWLIST — the one
// shape that is legitimately large — is normally a single keep rule with a long
// alternation, which is why ONE rule may spend the whole chain budget while the
// chain itself is held to it.
//
// The excess is REFUSED, never the monitor: rejecting the CR would take every
// target it contributes with it, which is a bigger outage than the chain is a
// risk. The refusal rides Endpoint.Ignored like every other clause kubescrape
// does not apply, so it counts into kubescrape_monitor_fields_ignored_total and
// names itself in the per-upsert warning instead of being silent.
//
// WHICH WAY EACH CEILING FAILS, stated rather than left to be rediscovered — a
// refused relabel rule is INVISIBLE in the data (the series it would have
// dropped simply arrive), so the direction is the whole of what an operator
// gets:
//
//   - The two AGGREGATE ceilings (maxRelabelRules, maxRelabelChainBytes) keep
//     the chain's PREFIX and refuse the tail, which FAILS OPEN for the refused
//     rules: a `keep` in the tail was an allowlist, and without it the target
//     exports everything the allowlist excluded. That is deliberate. The
//     prefix is the head of the operator's own chain applied in the operator's
//     own order, so it is strictly closer to the CR than nothing; the
//     merge-time sibling (scrape.MaxRelabelChainRules) has no other option at
//     all, since it must not refuse a target other monitors legitimately
//     created; and reaching either ceiling needs a chain no good-faith monitor
//     writes.
//   - The PER-RULE ceiling refuses the ENDPOINT — no targets at all — because
//     failing open there cannot be argued the same way. maxRelabelRuleBytes is
//     the whole chain budget, so a rule that trips it does not fit ANY chain,
//     in any order, and dropping it is not an approximation of the CR: it is
//     the one shape (a single enormous alternation) the legitimately-large
//     `keep` allowlist has, and honouring the endpoint without it is exactly
//     the silent inversion — export everything — that the rule was written to
//     prevent. Not scraping is the outcome an operator NOTICES; over-exporting
//     is the one that shows up on next month's bill. It is the same trade
//     enforceFieldBounds (bounds.go) makes for path/serverName/credentials, reached
//     through the same Refused field, and it is safe for the same reason: a
//     refused endpoint yields no target, so it cannot shadow, merge with or
//     upgrade anybody else's.
//
// maxRelabelRuleBytes therefore EQUALS maxRelabelChainBytes on purpose. It was
// half of it, which made a legitimate ~5 KiB allowlist — roughly 170 metric
// names — trip the per-rule ceiling while a chain of small rules could spend
// twice as much, and the answer to it was the silent fail-open above.
//
// BYTES ARE NOT COST, which is why a third quantity is bounded the same way.
// What a regex costs to parse and compile is not proportional to its text:
// `a{1000}` repeated to fill 8 KiB compiles to ~1.17M instructions (~350 MB
// allocated, ~52 MB retained per compiled regex), and a case-folded range or a
// Unicode class is expanded rune by rune or table by table while PARSING. So
// every admitted rule is also measured by kubemeta.RelabelRegexCost — the
// first half of the agent's own compile, so the two doors agree — and charged
// against maxRelabelChainCost exactly as its bytes are charged against
// maxRelabelChainBytes: one rule over the whole budget refuses the endpoint
// (relabelOversizeIgnored — the remedy is the same: shrink the regex), a
// chain of rules that together overflow it keeps its prefix. Only ADMITTED
// rules are measured, since measuring means parsing, so a rule past the
// aggregate ceilings is never charged. Measured through Parse, a 128-endpoint
// monitor whose every endpoint spends this budget AND relabelWrites' (see
// maxTrackedWrites) costs ~0.5 s and ~550 MB allocated, transiently, per
// upsert — the bounded residual under the default -monitor-namespaces —
// against ~50 s and ~45 GB with only the byte ceilings, and ~4 ms for the same
// monitor carrying ordinary rules.
const (
	maxRelabelRules = 64
	// One rule may spend the whole chain budget; over it is a refusal of the
	// endpoint, not of the rule.
	maxRelabelRuleBytes  = maxRelabelChainBytes
	maxRelabelChainBytes = 8 << 10
	// The same shape for cost: kubemeta refuses one regex over it, and the
	// chain may not spend more in total.
	maxRelabelChainCost = kubemeta.MaxRelabelRegexCost
)

// The Ignored entries the bounds above produce. Spelled with a parenthesised
// suffix like noPortIgnored, because they report a REFUSAL of something
// kubescrape does interpret rather than a field it ignores wholesale.
const (
	relabelCappedIgnored = "metricRelabelings" + cappedSuffix
	// relabelRefusedField is the Refused name for an endpoint carrying a rule
	// whose regex cannot be applied — too large, or not a regex at all — and
	// the two Ignored entries are built FROM it so the two spellings — the one
	// in Ignored and the one in Refused — cannot drift.
	relabelRefusedField    = "metricRelabelings.regex"
	relabelOversizeIgnored = relabelRefusedField + oversizeSuffix
	relabelInvalidIgnored  = relabelRefusedField + invalidSuffix
	// relabelDependentField is the Refused name for an endpoint whose applied
	// keep/drop rule READS a label an earlier, unapplied mutating rule writes
	// or removes (see relabelWrites), and relabelDependentIgnored its entry.
	relabelDependentField   = "metricRelabelings.action"
	relabelDependentIgnored = relabelDependentField + dependentSuffix
	// relabelDefaultActionIgnored reports a rule with NO action, which is
	// Prometheus' default replace, rather than echoing an empty value.
	relabelDefaultActionIgnored = "metricRelabelings.action=replace(default)"
	// The report's OWN ceiling; see maxRelabelIgnored.
	relabelReportCappedIgnored = "metricRelabelings(unsupported-capped)"
)

// invalidSuffix marks a report entry for a value kubescrape interprets but
// REFUSED because it is not well-formed (a regex that does not compile), and
// dependentSuffix one it refused because it depends on a clause kubescrape does
// not apply. Both refuse the endpoint, like oversizeSuffix.
const (
	invalidSuffix   = "(invalid)"
	dependentSuffix = "(dependent)"
)

// maxRelabelIgnored bounds the REPORT the walk below produces, which is the
// sibling of the bound on the rules it applies — and it is a separate bound for
// the reason this package keeps re-learning: the two arms that report an
// unsupported `action` or a custom `separator` embed a TENANT-CHOSEN string and
// skip the rule, so they never reach the rules ceiling however it is tuned, and
// bounding only the applied half moved the allocation instead of closing it.
// Measured through the real parser: a ~1.1 MiB CR fragment of 20,000 keep rules
// each with a distinct `separator` yields 0 applied rules, 20,000 Ignored
// entries (1,180,000 bytes retained on the endpoint in the index, forever) and
// one Warn line of the same order — re-emitted on every edit of the CR, since
// warnIgnored is gated on UpsertChanged's news.
//
// Past the ceiling the walk keeps applying rules it CAN apply (the report is a
// diagnostic; refusing to honour a valid rule because an earlier one was
// unreportable would be the wrong half to give up) and folds the rest of the
// report into one constant entry. A constant, not a count, because these
// entries reach warnIgnored through IgnoredFields, which dedupes: a per-endpoint
// tally would be a DISTINCT string per endpoint and the joined line would grow
// with the endpoint list — the same defect one level up.
//
// The ceiling bounds the two TENANT-ECHO arms and nothing else. The walk's own
// VERDICTS — the chain capped, the endpoint refused — are constants, each
// written at most once per walk, and they are appended past the ceiling rather
// than through it: routed through it, eight ordinary `labeldrop` rules ahead of
// an oversized `keep` allowlist left the endpoint refused while its report named
// only the labeldrops. So the report holds at most maxRelabelIgnored echoes, the
// one summary entry, and at most two verdicts (capped, then a refusal). A
// repeated echo is not charged twice either: eight identical `action: labeldrop`
// rules are one fact.
const maxRelabelIgnored = 8

// maxIgnoredValueBytes clips the tenant-chosen halves the two report arms echo
// (`action=`, `separator=`). Echoing the value is worth keeping — "action=Foo"
// is the whole diagnosis — but echoing it VERBATIM makes the log line and the
// retained report as large as the attacker's string, so it is clipped to a
// length that shows what was written without carrying it. The value is CRD
// free-form text, never secret material (secret-bearing fields are named by
// Endpoint.secretRefs and are reported by NAME only).
const maxIgnoredValueBytes = 48

// clipValue renders a tenant-supplied value for a report entry, cut at
// maxIgnoredValueBytes with an ellipsis so a reader can tell a clipped value
// from a short one. Cut on a rune boundary (internal/clip): the entry is
// written into a log record and a JSON document, and half a rune is a mojibake
// byte in both.
func clipValue(s string) string { return clip.Marked(s, maxIgnoredValueBytes, "...") }

// relabelLabelBytes is what ONE sourceLabels entry costs beyond its own
// characters: its JSON framing in the served node-targets document (two quotes
// and a comma) and its slice slot and per-sample visit in the agent's
// relabelFilter. Charging it is not tidiness — it is the difference between a
// bound on BYTES and a bound that a list of EMPTY strings walks straight past.
// Measured through the real parser and json.Marshal: one keep rule with 500,000
// empty sourceLabels is charged 2 bytes by the length-only accounting (the
// regex), is admitted whole by both ceilings, and marshals to 1,500,049 bytes
// in EVERY target the monitor resolves to — once per matched pod, per agent
// poll. The CR that carries it is ~1.5 MiB, i.e. inside etcd's object limit.
const relabelLabelBytes = 3

// RelabelRuleBytes is one rule's cost in the two places that make the chain
// worth bounding: the served target it is copied into, and the per-sample walk
// in the agent's relabelFilter. The action is one of two constants and is not
// charged.
//
// It is the ONE accounting for BOTH doors that bound a chain: this package's
// parse-time ceilings (relabelChain) and internal/scrape's merged-chain ceiling
// (scrape.MaxRelabelChainBytes, in appendRelabelChain). They bound the same
// quantity, and the merged ceiling's argument — a single endpoint's chain can
// never arrive at the merge already over it — holds only while both measure
// alike. They used to be two copies, held equal by a comment on each. Exported
// with a (regex, sourceLabels) signature rather than over the wire type because
// the parse side walks the CRD's decode shape, not kubemeta.RelabelRule.
func RelabelRuleBytes(regex string, sourceLabels []string) int {
	n := len(regex)
	for _, l := range sourceLabels {
		n += len(l) + relabelLabelBytes
	}
	return n
}

// relabelChain walks the endpoint's metricRelabelings ONCE and returns all
// three parts of the verdict: the rules kubescrape will apply, the report
// entries for every rule it refused, and — when the CHAIN cannot be honoured at
// all — the Refused field name that refuses the endpoint itself (see the
// ceilings above: the aggregate ones keep a prefix, the refusals below cannot).
// One walk, because a rule reported as dropped and then applied (or the
// reverse) is precisely the silent partial application the Ignored machinery
// exists to prevent — and one walk over the WHOLE list, because the oversize
// verdict is a property of the CR rather than of where in it the rule sits (see
// `capped` in the body).
//
// Three things refuse the endpoint, and all three for the reason the oversize
// ceiling gives: the chain that would be shipped is not an approximation of the
// CR, so not scraping — the outcome an operator notices — beats scraping
// through a filter nobody wrote.
//
//   - A rule over the whole chain budget (relabelOversizeIgnored): its BYTES
//     measured on every rule wherever it sits, its compile COST
//     (kubemeta.RelabelRegexCost) on every admitted rule.
//   - An ADMITTED rule whose regex does not compile (relabelInvalidIgnored).
//     The agent compiles the chain it is served and FAILS the scrape on a bad
//     regex rather than export what the user asked to drop — and
//     scrape.MergeMonitorEndpoint concatenates the chains of every monitor
//     resolving to one URL, so admitting it here let ONE tenant's typo fail
//     another monitor's merged target on every agent, every cycle, while the
//     counter, the warning and /v1/explain all read clean. Refused, the
//     endpoint yields no target and can merge into nobody's. Admitted rules
//     are PARSED, never compiled (a parse is where every compile error comes
//     from), and only admitted ones: a rule past the aggregate ceilings is
//     never applied, so it cannot poison anything. The 8 KiB byte budget does
//     not by itself make that parse cheap — see maxRelabelChainCost — so the
//     chain's cost budget is what bounds it: the walk parses at most the
//     budget's worth plus the one rule that overflows it.
//   - An ADMITTED keep/drop that reads a label an earlier label-MUTATING rule
//     — one kubescrape does not apply — would have written or removed
//     (relabelDependentIgnored; see relabelWrites). `replace` into
//     `service`, then `keep` on `service`: applying the keep alone tests the
//     user's regex against a label that was never rewritten, which drops
//     every series while the target reports up=1 — and the only report was
//     "action=replace has no effect".
func (ep endpointSpec) relabelChain() (rules []RelabelRule, ignored []string, refused string) {
	chainBytes, chainCost := 0, 0
	// report appends one TENANT-ECHO entry under maxRelabelIgnored, folding
	// everything past it into the one constant entry. It is a closure rather
	// than two call sites' worth of `if len(echoes) < …` because the arms it
	// guards are exactly the ones the applied-rule ceilings cannot reach. The
	// walk's VERDICTS never go through it (see maxRelabelIgnored), and an entry
	// already reported is not charged again.
	echoes, reportCapped := 0, false
	report := func(entry string) {
		switch {
		case slices.Contains(ignored, entry):
		case echoes < maxRelabelIgnored:
			echoes++
			ignored = append(ignored, entry)
		case !reportCapped:
			reportCapped = true
			ignored = append(ignored, relabelReportCappedIgnored)
		}
	}
	// refuse ends the walk with the endpoint refused: the chain is discarded
	// either way (enforceFieldBounds nils it), so the prefix is dropped rather
	// than returned, and the entry is appended directly — it is the one thing
	// an operator must be told.
	refuse := func(entry, field string) ([]RelabelRule, []string, string) {
		return nil, append(ignored, entry), field
	}
	// capped is set once an AGGREGATE ceiling has bound. The chain is closed
	// from then on, but the WALK CONTINUES, and that is the difference between
	// the two ceilings being what they are documented to be and one of them
	// silently becoming the other. The per-rule ceiling refuses the ENDPOINT
	// precisely because a rule over it fits no chain IN ANY ORDER — so its
	// position must not decide the verdict. It did: the aggregate ceilings
	// stopped the walk, so an oversized `keep` allowlist placed past the 64th
	// rule (or past the 8 KiB) was never measured, `oversize` stayed false, the
	// endpoint kept its Port, and every target it resolved to was served with
	// the prefix applied and the allowlist silently absent — export everything,
	// the exact fail-OPEN the per-rule ceiling exists to prevent, reported as
	// the fail-open the aggregate ceilings are DOCUMENTED to be so no counter
	// or /v1/explain document could tell them apart.
	//
	// Continuing costs one RelabelRuleBytes per remaining rule over a list the
	// decode already materialised, once per monitor upsert. The two report arms
	// below stay silent past the ceiling, so the report is unchanged: a capped
	// chain is one entry, not one per unread rule. Nothing past the ceiling is
	// APPLIED, so nothing past it is parsed or dependency-checked either.
	capped := false
	var writes relabelWrites
	for _, r := range ep.MetricRelabelings {
		if !isKeepDrop(r.Action) {
			if !capped {
				action, entry := strings.ToLower(r.Action), "metricRelabelings.action="+clipValue(r.Action)
				if action == "" {
					// An ABSENT action is Prometheus' default, replace. A
					// current CRD fills it in on read; one that predates its
					// `default: replace` marker hands the informer "" — and
					// read as no action at all, a replace into a label a later
					// keep reads went untracked and the keep was applied alone.
					action, entry = "replace", relabelDefaultActionIgnored
				}
				report(entry)
				writes.note(action, r.TargetLabel, r.Regex)
			}
			continue
		}
		// A custom separator is reported and the rule SKIPPED when it would
		// actually be WRITTEN — two or more sourceLabels: the agent joins
		// sourceLabels with ';', so applying the rule anyway would test the
		// user's regex against a string it was never written for — silently
		// inverting a keep into a drop-everything. Skipping it fails OPEN (the
		// series it would have filtered arrive), which is a stated residual of
		// this arm and is why it is reported. With zero or one sourceLabels
		// no separator is ever written — by the agent (it writes one only
		// between two values) or by Prometheus — so the join is identical and
		// the rule is applied exactly as the CR says.
		if r.Separator != "" && r.Separator != ";" && len(r.SourceLabels) > 1 {
			if !capped {
				report("metricRelabelings.separator=" + clipValue(r.Separator))
			}
			continue
		}
		// Measured BEFORE the aggregate ceilings and on EVERY rule, capped or
		// not — see `capped` above for why the order and the continuation are
		// both load-bearing.
		n := RelabelRuleBytes(r.Regex, r.SourceLabels)
		if n > maxRelabelRuleBytes {
			// One rule bigger than the WHOLE chain budget refuses the endpoint
			// (toEndpoint, through enforceFieldBounds): skipping it and
			// applying its neighbours ships a filter the CR does not describe,
			// and for the one shape that is legitimately this large — a `keep`
			// allowlist — that means exporting everything it excluded.
			return refuse(relabelOversizeIgnored, relabelRefusedField)
		}
		if capped {
			continue
		}
		// Over the aggregate bounds: the PREFIX of the chain is kept and the
		// rest refused. A prefix rather than nothing because relabel rules are
		// independent filters applied in order — keeping the ones written
		// first is strictly closer to the CR than keeping none — and because a
		// legitimate chain never reaches here at all. The entry is a verdict,
		// written directly and exactly once (capped latches).
		if len(rules) >= maxRelabelRules || chainBytes+n > maxRelabelChainBytes {
			ignored = append(ignored, relabelCappedIgnored)
			capped = true
			continue
		}
		if slices.ContainsFunc(r.SourceLabels, writes.touches) {
			return refuse(relabelDependentIgnored, relabelDependentField)
		}
		// The first half of the agent's own compile
		// (kubemeta.CompileRelabelRegex runs it before compiling), so the two
		// doors cannot disagree about what a regex is or what is too large —
		// and it parses without compiling.
		cost, err := kubemeta.RelabelRegexCost(r.Regex)
		switch {
		case errors.Is(err, kubemeta.ErrRelabelRegexTooLarge):
			return refuse(relabelOversizeIgnored, relabelRefusedField)
		case err != nil:
			return refuse(relabelInvalidIgnored, relabelRefusedField)
		case chainCost+cost > maxRelabelChainCost:
			ignored = append(ignored, relabelCappedIgnored)
			capped = true
			continue
		}
		chainCost += cost
		chainBytes += n
		rules = append(rules, RelabelRule{
			// Normalized, so everything downstream compares one spelling.
			Action:       strings.ToLower(r.Action),
			SourceLabels: r.SourceLabels,
			Regex:        r.Regex,
		})
	}
	return rules, ignored, ""
}

// maxTrackedWrites bounds how many distinct label-mutating rules relabelWrites
// tracks precisely, per list (exact names; name predicates). The rules it
// tracks are UNAPPLIED, so the chain ceilings do not bound them, and a
// predicate costs a regex compile that is HELD for the whole walk — so the
// predicates' summed kubemeta.RelabelRegexCost is bounded as well, by the same
// maxRelabelChainCost an applied chain gets. Without it, 64 labeldrops of
// `a{1000}` repeated to 8 KiB held ~3.3 GB of compiled programs at once, on
// every parse of the CR, in the metadata-service singleton. Past either bound
// the walk stops being precise and treats every label as written — which
// refuses a dependent keep/drop that follows, i.e. fails CLOSED, and no
// good-faith chain carries 64 mutating rules, or one costly predicate, ahead
// of its filters.
const maxTrackedWrites = maxRelabelRules

// relabelWrites is what the label-MUTATING rules a chain carries — each one
// skipped, because kubescrape applies only keep/drop — would have done to the
// label set a LATER keep/drop reads. Prometheus applies a chain in order, so a
// filter after a `replace` sees the replaced value; kubescrape applies the
// filter to the label set the target exported, and those two differ exactly on
// the labels recorded here.
//
// Precise where it cheaply can be, because the common chain must not be
// refused: `labeldrop` of an unwanted label followed by a `drop` on __name__ is
// ordinary, and a `labeldrop` names the labels it removes by a regex over NAMES
// that can be evaluated here. Conservative where it cannot: a `labelmap` writes
// names computed from the label set itself, a `replace` target containing `$`
// is templated from the matched value, and a regex that does not compile (or
// is over the per-rule budget, or past maxTrackedWrites or the predicates'
// cost budget) cannot be evaluated — each of those is treated as writing every
// label.
type relabelWrites struct {
	all   bool
	names []string
	// preds are the labeldrop/labelkeep rules: a label is touched when the
	// anchored regex matching its NAME equals removes (labeldrop removes the
	// names it matches, labelkeep the names it does not).
	preds []namePredicate
	// cost is the preds' summed kubemeta.RelabelRegexCost; see
	// maxTrackedWrites.
	cost int
}

type namePredicate struct {
	re      *regexp.Regexp
	removes bool
}

// note records one skipped rule. action is lower-cased. Unknown actions, and
// the filtering actions kubescrape does not apply (keepequal, dropequal), write
// nothing; a rule Prometheus itself would reject (a `replace` with no
// targetLabel) writes nothing either, since there is then no chain to match.
func (w *relabelWrites) note(action, targetLabel, regex string) {
	if w.all {
		return
	}
	switch action {
	case "replace", "hashmod", "lowercase", "uppercase":
		switch {
		case targetLabel == "":
		case strings.Contains(targetLabel, "$") || len(w.names) >= maxTrackedWrites:
			w.all = true
		case !slices.Contains(w.names, targetLabel):
			w.names = append(w.names, targetLabel)
		}
	case "labeldrop", "labelkeep":
		if len(w.preds) >= maxTrackedWrites || len(regex) > maxRelabelRuleBytes {
			w.all = true
			return
		}
		// Measured BEFORE it is compiled: the compile is what the cost bound
		// exists to refuse.
		cost, err := kubemeta.RelabelRegexCost(regex)
		if err != nil || w.cost+cost > maxRelabelChainCost {
			w.all = true
			return
		}
		re, err := kubemeta.CompileRelabelRegex(regex)
		if err != nil {
			w.all = true
			return
		}
		w.cost += cost
		w.preds = append(w.preds, namePredicate{re: re, removes: action == "labeldrop"})
	case "labelmap":
		w.all = true
	}
}

// touches reports whether a skipped mutating rule would have written or
// removed the label a keep/drop reads ("__name__" included, which `replace`
// can target and `labeldrop` can remove).
func (w *relabelWrites) touches(label string) bool {
	if w.all || slices.Contains(w.names, label) {
		return true
	}
	for _, p := range w.preds {
		if p.re.MatchString(label) == p.removes {
			return true
		}
	}
	return false
}
