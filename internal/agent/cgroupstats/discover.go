package cgroupstats

// Finding the container cgroups, without assuming a layout.
//
// The tree differs by node, and every difference is load-bearing:
//
//	kind (systemd driver, --cgroup-root=/kubelet.slice):
//	  <root>/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-besteffort.slice/
//	         kubelet-kubepods-besteffort-pod<uid_with_underscores>.slice/
//	         cri-containerd-<64hex>.scope
//	stock systemd driver:
//	  <root>/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod<uid_>.slice/
//	         cri-containerd-<64hex>.scope
//	cgroupfs driver:
//	  <root>/kubepods/burstable/pod<uid>/<64hex>
//
// plus the Guaranteed QoS class, which has no QoS segment at all. Hardcoding
// any one of these is how a sampler ships that works on the author's cluster
// and silently finds nothing on everyone else's — which is the failure mode
// this whole feature exists to make impossible for CPU bursts, so it would be a
// poor one to build in.
//
// Segment PARSING is not re-implemented: pkg/cgroupid already understands both
// driver layouts (it is what turns cadvisor's `id` label into a pod uid and a
// container id) and its Parse returns exactly the shape verdict discovery
// needs — "this segment is a container scope that is the immediate child of a
// pod slice, and the path's last segment". Discovery only has to decide WHICH
// directories to hand it.

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/cgroupid"
)

const (
	// maxRootSearchDepth is how deep below the cgroup root the kubepods slice
	// is looked for. kind needs 1 (kubelet.slice/kubelet-kubepods.slice); a
	// stock node needs 0. Two is a margin for another wrapper slice, not an
	// invitation to walk the machine.
	maxRootSearchDepth = 2
	// maxPodDepth is how deep below a kubepods root a container scope can sit:
	// QoS slice → pod slice → container scope. Guaranteed pods use two.
	maxPodDepth = 3
	// maxScanDirs bounds one discovery pass. The tree is small on any sane
	// node; the bound is there so a pathological or hostile hierarchy cannot
	// stall the discovery goroutine, and it is counted when it binds.
	maxScanDirs = 8192
)

// containerDir is one discovered container cgroup.
type containerDir struct {
	id     string // 64-hex runtime container id
	podUID string
	path   string
}

// discover re-reads the container set, reconciles it against what is tracked,
// and resolves the identity of anything new. It runs on the DISCOVERY
// goroutine, never on the sampler's (see the Sampler doc for the stall that
// sharing produced).
//
// The two phases are separate because they have opposite locking needs:
// reconciliation touches the tracked set and must hold the mutex the sample
// sweep uses, while resolution is a metadata lookup that can block for as long
// as the metadata service takes and must NOT be holding it.
func (s *Sampler) discover(ctx context.Context, now time.Time) {
	found, complete, ok := s.walk()
	if !ok {
		return
	}
	s.reconcile(found, complete)
	s.resolvePending(ctx, now)
	s.warnIfExportingNothing()
}

// walk lists the container cgroups under the root, reporting whether the
// listing was COMPLETE.
//
// Completeness is not a detail: an incomplete listing is indistinguishable from
// "these containers are gone", and acting on it retires live containers,
// discarding their window and their CPU baseline (and, before this returned the
// flag at all, without counting anything — the pipeline just quietly lost data
// whenever a pod slice could not be read). It is the same rule the tailer
// applies to its checkpoint pruning: a failed listing proves nothing, so prune
// on a SUCCEEDED one only.
func (s *Sampler) walk() (found []containerDir, complete, ok bool) {
	found, complete, err := discoverContainers(s.root)
	if err != nil {
		// The ROOT itself: nothing was discovered and nothing will be exported.
		// Counted apart from the partial-listing case below because the two
		// have nothing in common but the word "discovery" — this one is a
		// missing mount and a silent pipeline, that one is a directory the
		// sampler could not read inside an otherwise working hierarchy.
		s.c.listErrRoot.Inc()
		s.log.Warn("cgroup discovery failed", "root", s.root, "error", err)
		return nil, false, false
	}
	if !complete {
		s.c.listErrSubtree.Inc()
		if s.listWarn.Allow(readWarnEvery) {
			s.log.Warn("cgroup discovery could not list part of the hierarchy (throttled); nothing is retired this pass, since an unreadable directory is indistinguishable from a container that went away",
				"root", s.root)
		}
	}
	return found, complete, true
}

// reconcile brings the tracked and pending sets in line with one listing.
func (s *Sampler) reconcile(found []containerDir, complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.seen)
	for _, d := range found {
		s.seen[d.id] = struct{}{}
		base := len(filepath.Base(d.path))
		if c, ok := s.tracked[d.id]; ok {
			if d.path != c.dir && base < c.baseLen {
				s.repointLocked(c, d, base)
			}
			continue
		}
		if p, ok := s.pending[d.id]; ok {
			if d.path != p.dir && base < p.baseLen {
				p.dir, p.baseLen = d.path, base
			}
			continue
		}
		// The PENDING cap, which is the memory one. The fd cap is checked
		// where descriptors are actually spent (track); testing it here spent
		// a descriptor budget on entries that hold no descriptor, and since
		// every pod's sandbox cgroup is permanently pending, a large node's
		// sandboxes crowded its real containers out of the sampled set.
		if len(s.pending) >= s.maxPending {
			s.c.cappedPending.Inc()
			if !s.capped {
				s.capped = true
				s.log.Warn("cgroup sampler is at its unresolved-cgroup cap; further cgroups are not even offered to the resolver",
					"cap", s.maxPending, "root", s.root)
			}
			continue
		}
		s.pending[d.id] = &pendingContainer{
			id: d.id, podUID: d.podUID, dir: d.path, baseLen: base,
		}
	}

	if complete {
		for _, c := range s.tracked {
			if _, ok := s.seen[c.id]; ok {
				continue
			}
			// The cgroup is gone. Its descriptors are released NOW (nothing
			// readable is left behind them, and a closed fd number is reusable
			// by any other goroutine's open, so they must not stay in a struct
			// the sweep walks), but the container itself is kept until the next
			// export carries the window it accumulated. That window is the
			// OOM-killed container's last seconds — the burst this package
			// exists to preserve — and dropping it here threw it away at
			// exactly the moment it mattered most. The final flush still
			// rebuilds the identity like any other, and it still resolves: the
			// metadata service keeps a deleted pod resolvable by container id
			// for its whole -cache-ttl, which is what that tombstone is for.
			c.markGone()
		}
		for id, p := range s.pending {
			if _, ok := s.seen[id]; !ok {
				delete(s.pending, p.id)
			}
		}
	}
	if len(s.pending) < s.maxPending {
		s.capped = false
	}
	s.publishCountsLocked()
}

// repointLocked moves a tracked container to a better cgroup path.
//
// CRI-O creates crio-conmon-<id>.scope — the SUPERVISOR's cgroup, carrying the
// container's own id in its name — as a sibling of the container's scope, and
// it can exist BEFORE the container's own does. discoverContainers prefers the
// shorter basename whenever both are present in one listing, but a pass that
// saw only the conmon scope used to latch it for the container's whole life:
// the reported working set was the supervisor's few MiB instead of the
// workload's hundreds, under the workload's identity, with nothing to indicate
// it. So a tracked container follows a BETTER path when one appears.
//
// Only ever to a better one. Late in a container's life the ordering reverses
// (its own scope is removed while conmon lingers), and following the listing
// down to the supervisor there would reproduce the same defect at the other end
// of the lifecycle. Reads against the removed cgroup fail and are counted until
// the id leaves the listing altogether, which retires it.
//
// The accumulated window and the CPU baseline are DISCARDED: they measured a
// different cgroup. Keeping them would fold the supervisor's numbers into the
// container's distribution and, worse, derive one enormous rate across the
// switch from two unrelated counters.
func (s *Sampler) repointLocked(c *container, d containerDir, base int) {
	s.log.Info("cgroup sampler re-pointed a container at a better cgroup path (a supervisor scope carrying the same container id was seen first)",
		"container", c.id, "from", c.dir, "to", d.path)
	fds, err := openCgroupFDs(d.path)
	if err != nil {
		obs.CgroupOpenErrors.Inc()
		return // keep what we have; the next pass retries
	}
	c.release()
	c.fds, c.open = fds, true
	c.dir, c.baseLen = d.path, base
	c.cpu.reset()
	c.mem.reset()
	c.heldCPU, c.heldMem = held{}, held{}
	c.havePrev = false
}

// resolvePending asks the resolver for the identity of every cgroup that does
// not have one yet, and starts sampling the ones that get an answer.
//
// This seam is a GATE, not the source of the exported resource: what it decides
// is whether to spend three file descriptors and a slot in the sampled set on a
// cgroup. The resource itself is rebuilt from current metadata at every export
// (see the package doc and Sampler.resolveExport), so nothing built here is
// kept — the answer that matters is the bool.
//
// An unresolved container is NOT SAMPLED at all, which is the same rule the
// export applies one seam later. A series attributed to nothing does not join
// the cadvisor series it explains, which is the only reason these ten gauges
// exist; declining at discovery too means the sampler never holds descriptors,
// never reads, and never accumulates a window for a container whose data can
// never be exported. It also removes the pod SANDBOX for free: the pause
// container's id appears in no pod's containerStatuses, so the metadata service
// cannot resolve it, so the phantom second resource every pod would otherwise
// mint never materialises — no sandbox predicate needed beside the one
// cadvisorbatch.go already carries.
//
// The pass is BUDGETED. A node full of cgroups whose metadata service has gone
// slow would otherwise hold this goroutine for as long as every lookup takes,
// pushing the next pass out behind it and starving the just-started containers
// waiting to be sampled; what does not fit rolls to the next pass. (The budget
// no longer protects the SWEEP — that is what moving discovery onto its own
// goroutine did — but it still bounds how far behind the container set may
// get.) Which is exactly why maxUnresolvedAge is measured from the first
// ANSWERED FAILURE rather than from discovery: a rolled-over entry has not been
// asked about yet, and abandoning it for a grace period it never got to spend
// would turn one slow minute into a node-wide drop to the ten-minute cadence.
func (s *Sampler) resolvePending(ctx context.Context, now time.Time) {
	s.mu.Lock()
	s.todo = s.todo[:0]
	for _, p := range s.pending {
		if !s.dueLocked(p, now) {
			continue
		}
		s.todo = append(s.todo, pendingSnap{id: p.id, podUID: p.podUID, lastTry: p.lastTry})
	}
	s.mu.Unlock()

	// LEAST RECENTLY TRIED first, id breaking ties. The order matters only when
	// the budget below cuts the pass short, and then it matters a lot: a Go map
	// range is randomised, so the truncated prefix used to be a fresh lottery
	// every pass, and a container could lose it many times in a row while
	// another was asked about repeatedly. Oldest-first rotates instead, so a
	// slow metadata service delays every pending cgroup a little rather than
	// starving an unlucky few indefinitely — and a never-tried entry (zero
	// time) goes to the front, which is what gets a just-started container
	// sampled promptly on a busy node.
	slices.SortFunc(s.todo, func(a, b pendingSnap) int {
		if c := a.lastTry.Compare(b.lastTry); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})

	deadline := now.Add(s.resolveBudget)
	for i := range s.todo {
		t := s.todo[i]
		// The FIRST is always attempted, whatever the budget says: a pass that
		// resolves nothing can never make progress, and the budget exists to
		// bound a slow service, not to stop one.
		if i > 0 && !s.now().Before(deadline) {
			break
		}
		res := pcommon.NewResource()
		rctx, cancel := context.WithTimeout(ctx, resolveTimeout)
		ok, answered := s.resolver.FillContainerResource(rctx, res, t.id, t.podUID)
		cancel()
		if ok {
			s.track(t.id, now)
		} else {
			s.resolveFailed(t.id, now, answered)
		}
	}
}

// dueLocked reports whether a pending cgroup should be offered to the resolver
// on this pass. Caller holds mu.
//
// Everything not given up on is due every pass; a cgroup that HAS been given up
// on is due when its own clock says so. The whole retry policy therefore lives
// in the one place that knows WHY the last attempt failed (resolveFailed): a
// definitive miss sets nextTry ten minutes out, a service that could not answer
// sets it one minute out. There is no second, cross-entry arm — the earlier
// design had one (any successful lookup anywhere re-armed the whole abandoned
// set), and on a node that exports anything it was satisfied permanently, which
// silently replaced the ten-minute cadence with the one-minute floor for every
// sandbox on the node. See reconsiderEvery.
func (s *Sampler) dueLocked(p *pendingContainer, now time.Time) bool {
	return !p.gaveUp || !now.Before(p.nextTry)
}

// track promotes a resolved cgroup into the sampled set.
//
// This is where the FILE DESCRIPTOR cap binds, because this is where the
// descriptors are spent. A refused container stays pending and is retried, so
// it takes the slot of whichever sampled container retires first rather than
// being lost.
func (s *Sampler) track(id string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[id]
	if !ok {
		return // it went away while the lookup was in flight
	}
	// A lookup HAPPENED, whatever comes of it. Leaving lastTry alone on the
	// refusal below kept the entry's zero value, which sorts to the front of
	// every budget-truncated pass forever — re-creating, at the cap, exactly
	// the starvation the least-recently-tried ordering was added to fix (the
	// refused entry is asked about again and again while the containers behind
	// it in the queue never are).
	p.lastTry = now
	if len(s.tracked) >= s.maxTracked {
		s.c.cappedTracked.Inc()
		if !s.cappedFDs {
			s.cappedFDs = true
			s.log.Warn("cgroup sampler is at its container cap; further containers are resolved but not sampled",
				"cap", s.maxTracked, "root", s.root)
		}
		return
	}
	s.cappedFDs = false
	// Three opens under the mutex the sweep uses. They are microseconds, and
	// doing them outside it would mean re-checking every piece of state they
	// were decided from.
	fds, err := openCgroupFDs(p.dir)
	if err != nil {
		// The container went away between the listing and the open, or its
		// controllers are not enabled on this cgroup. Not worth a line per
		// cycle; it stays pending and the next pass retries.
		obs.CgroupOpenErrors.Inc()
		return
	}
	delete(s.pending, id)
	s.tracked[id] = &container{
		id: id, podUID: p.podUID, dir: p.dir, baseLen: p.baseLen,
		fds: fds, open: true,
	}
	s.publishCountsLocked()
}

// resolveFailed records one unsuccessful resolution and, past the grace period,
// gives up on the cgroup — but only on the strength of failures that say
// something about the CGROUP rather than about the metadata service.
//
// Giving up is what keeps a container that will NEVER resolve from being
// retried forever — every pod's sandbox is exactly that, and so is any
// non-Kubernetes container parked in the hierarchy. It is a give-up and not a
// deletion: the entry stays, retried at a far slower cadence.
//
// answered is the whole policy (see Resolver):
//
//   - A DEFINITIVE miss (404) is evidence about this cgroup. It starts and
//     feeds the grace-period clock, and once that runs out the entry is
//     abandoned to abandonRetryEvery — one lookup per pod per ten minutes for
//     the sandbox set, and no file descriptors at all.
//   - A miss the service could not answer is evidence about nothing. It never
//     starts the clock and never abandons anything, so a metadata outage cannot
//     cost a node's real containers their whole lifetime; and if it lands on an
//     entry that was ALREADY abandoned, the give-up loses its justification and
//     the entry drops back to reconsiderEvery. That is what makes the node
//     resume within a minute of the service returning, without any cross-entry
//     evidence tally.
func (s *Sampler) resolveFailed(id string, now time.Time, answered bool) {
	if answered {
		s.c.unresolvedPending.Inc()
	} else {
		s.c.unresolvedUnreachable.Inc()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.pending[id]
	if !ok {
		return
	}
	p.lastTry = now
	if !answered {
		// The service is the problem, not this cgroup. Keep whatever the
		// grace-period clock had (an outage in the middle of a run of 404s does
		// not buy a sandbox a fresh grace period), abandon nothing new, and put
		// an already-abandoned entry back on the fast clock.
		if p.gaveUp {
			p.nextTry = now.Add(reconsiderEvery)
		}
		return
	}
	if p.firstFail.IsZero() {
		// The abandonment clock starts at the first ANSWERED failure, not at
		// discovery: a budgeted pass can leave a cgroup waiting minutes for its
		// first attempt, and abandoning it on that attempt would spend a grace
		// period it never had. See maxUnresolvedAge.
		p.firstFail = now
	}
	if !p.gaveUp && now.Sub(p.firstFail) >= maxUnresolvedAge {
		p.gaveUp = true
		s.c.unresolvedAbandoned.Inc()
		s.log.Debug("cgroup sampler gave up resolving a container cgroup; it is not exported (one per pod is expected — the sandbox/pause cgroup appears in no pod's containerStatuses)",
			"cgroup", p.dir, "podUID", p.podUID, "retryIn", abandonRetryEvery)
	}
	if p.gaveUp {
		p.nextTry = now.Add(abandonRetryEvery)
	}
}

// discoverContainers walks root and returns every container cgroup under it,
// plus whether the listing was complete (see Sampler.walk).
func discoverContainers(root string) ([]containerDir, bool, error) {
	sc := &scan{root: root, budget: maxScanDirs, byID: map[string]int{}, complete: true,
		buf: make([]byte, dirBufBytes)}
	roots, err := sc.kubepodsRoots()
	if err != nil {
		return nil, false, err
	}
	for _, kp := range roots {
		sc.walkPods(kp, 0)
	}
	if sc.budget <= 0 {
		obs.CgroupScanTruncated.Inc()
		sc.complete = false
	}
	return sc.out, sc.complete, nil
}

// scan is one discovery pass: the shared directory budget, the accumulating
// result and whether everything it wanted to read could be read.
type scan struct {
	root   string
	budget int
	out    []containerDir
	// byID is what makes a helper scope parked beside a container (CRI-O's
	// crio-conmon-<id>.scope, an immediate child of the pod slice whose last
	// dash-separated component is the very same container id) not become a
	// SECOND entry for one container — which would export two resources with
	// identical identity in one payload. The container's own scope always has
	// the shorter basename, the helper's being the same name with an infix, so
	// the shortest wins and the choice is deterministic across passes.
	byID     map[string]int
	complete bool
	// buf is the getdents64 scratch shared by every directory of the pass; see
	// readdir_linux.go for why the listing does not go through os.ReadDir.
	buf []byte
}

func (sc *scan) walkPods(dir string, depth int) {
	if depth > maxPodDepth || sc.budget <= 0 {
		return
	}
	names, err := subdirs(dir, sc.buf)
	if err != nil {
		sc.complete = false
		return
	}
	sc.budget--
	for _, name := range names {
		p := filepath.Join(dir, name)
		rel, err := filepath.Rel(sc.root, p)
		if err != nil {
			continue
		}
		// The path is offered RELATIVE to the cgroup root: cgroupid.Parse walks
		// segments and counts them, and the mount point's own segments
		// (/sys/fs/cgroup) are not part of the kubelet's naming.
		podUID, cid, isContainer := cgroupid.Parse(rel)
		if isContainer {
			if j, ok := sc.byID[cid]; ok {
				if len(name) < len(filepath.Base(sc.out[j].path)) {
					sc.out[j] = containerDir{id: cid, podUID: podUID, path: p}
				}
				continue
			}
			sc.byID[cid] = len(sc.out)
			sc.out = append(sc.out, containerDir{id: cid, podUID: podUID, path: p})
			// A container scope has no container children.
			continue
		}
		sc.walkPods(p, depth+1)
	}
}

// kubepodsRoots finds the directories the kubelet parents its pod cgroups
// under. Usually one; a list because nothing guarantees it.
//
// The search is bounded in BOTH directions, which is the part worth reading.
// The cgroup root's own children are walked whole (there are a dozen of them,
// and a custom --cgroup-root can be named anything, so a name filter at the top
// would lose exactly the non-default layouts this function exists for), but
// below that a directory is only descended into if its name looks like the
// kubelet's — otherwise system.slice's hundred-odd unit directories, and every
// unit's children, get read on every discovery pass for nothing.
func (sc *scan) kubepodsRoots() ([]string, error) {
	if isKubepodsSlice(filepath.Base(sc.root)) {
		return []string{sc.root}, nil
	}
	if sc.budget <= 0 {
		return nil, nil
	}
	// The ROOT's own listing is the one error worth REPORTING rather than
	// merely flagging: it is what an unmounted or unreadable
	// -cgroup-stats-root looks like, and the whole pipeline is silent without
	// it. So it is read HERE, once, outside the recursion — which is what lets
	// everything below it be error-free by construction. (It used to be probed
	// with a SECOND full listing of the root before the walk read it again; one
	// listing answers both questions.)
	top, err := subdirs(sc.root, sc.buf)
	if err != nil {
		return nil, err
	}
	sc.budget--

	// Below the root a failed listing is an INCOMPLETE pass (sc.complete),
	// which retires nothing, and never an error — so descend returns none.
	// This used to be one recursive func returning error with a `depth == 0`
	// arm inside it, and since every recursive call passes depth+1 that arm was
	// reachable only from the outermost call: the error plumbing threaded
	// through the loop could not fire, and read as if a subtree failure might
	// abort the pass, which is the opposite of the rule above.
	var out []string
	var descend func(dir string, depth int)
	visit := func(dir string, depth int, names []string) {
		for _, name := range names {
			p := filepath.Join(dir, name)
			if isKubepodsSlice(name) {
				out = append(out, p)
				continue // pods live below it; nothing above them is another root
			}
			if depth == 0 || strings.Contains(name, "kube") {
				descend(p, depth+1)
			}
		}
	}
	descend = func(dir string, depth int) {
		if depth > maxRootSearchDepth || sc.budget <= 0 {
			return
		}
		names, err := subdirs(dir, sc.buf)
		if err != nil {
			sc.complete = false
			return
		}
		sc.budget--
		visit(dir, depth, names)
	}
	visit(sc.root, 0, top)
	return out, nil
}

// isKubepodsSlice reports whether a cgroup directory name is the kubelet's pod
// root. Three spellings, one rule: a systemd slice is named for its whole
// ancestry with dashes ("kubelet-kubepods.slice" lives under
// "kubelet.slice"), so the pod root is the segment whose LAST component is
// "kubepods" — which covers the cgroupfs "kubepods", the stock
// "kubepods.slice" and any --cgroup-root prefix, kind's "kubelet-" included.
func isKubepodsSlice(name string) bool {
	name = strings.TrimSuffix(name, ".slice")
	return name == "kubepods" || strings.HasSuffix(name, "-kubepods")
}
