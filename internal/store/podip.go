package store

// The pod-IP index: which live pod holds an address (byPodIP), every record
// currently reporting it (ipClaimants), and the one precedence rule that
// decides between them — the claim, release and promotion paths, and the
// address enumeration all of them key on.

import (
	"slices"

	"github.com/JohanLindvall/kubescrape/internal/peerip"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// claimPodIPLocked maintains the live-pod IP index for one upsert: hostNetwork
// and finished pods never claim, a stale old mapping is dropped (identity-
// checked), and among the rest the LATER ACQUIRER holds the address — pod IPs
// recycle, and a stale pod's routine status updates still carry the IP the CNI
// already handed to someone else. That covers a late-scheduled OLDER pod
// legitimately taking a freed IP, and a drainer keeping its address until it is
// actually released (claimOneIPLocked says why the terminating bit orders
// nothing here). noEvidence marks a claim that carries no acquisition evidence
// at all (see claimOneIPLocked).
func (s *Store) claimPodIPLocked(rec *record, pod kubemeta.Pod, oldIPs []string, noEvidence bool) {
	// EVERY address the pod reports. On a dual-stack cluster a connection can
	// arrive from the family status.podIP does not carry, and indexing only
	// that one left /v1/self and /v1/pod-ips unresolvable for it — the agent's
	// self-attribution and the ingest peer-IP fallback silently off, with a 404
	// indistinguishable from any other.
	addrs := podAddresses(pod)
	for _, ip := range addrs {
		s.claimOneIPLocked(rec, ip, oldIPs, noEvidence)
	}
	// Addresses the pod no longer reports are released.
	for _, old := range oldIPs {
		if old == "" || slices.Contains(addrs, old) {
			continue
		}
		s.releaseIPLocked(rec, old)
	}
}

// rawIPs is the ONE enumeration of the addresses a pod's status reports:
// status.podIPs, falling back to the single status.podIP when the list is
// empty (kubeconvert fills PodIPs today; the backstop covers records built
// before the field existed). Both podAddresses (claim eligibility) and
// recordAddresses (release/cleanup) MUST read this same list — a claim taken
// from an address one enumeration sees and the other does not is never
// released, the dual-stack leak class deletePodLocked's comment describes.
//
// Every address is CANONICALISED, because this is what the index is keyed by
// while every lookup arrives in peerip's form: /v1/self from the connection's
// source address, the agent's ingest fallback through metaclient.PodByIP. A
// kubelet or CNI reporting `::ffff:10.1.2.3`, `FD00::0:7` or a zoned address
// would otherwise index the pod under a key no lookup can ever form, and both
// of those paths would 404 for it — indistinguishable from any other miss. The
// SERVED model keeps the verbatim strings; only the keys are normalised.
//
// The copy is made only when an address is NOT already canonical, which on
// every cluster that spells its addresses the ordinary way is never. This runs
// on the informer goroutine holding the store's EXCLUSIVE lock — twice per
// upsert (the record's old addresses and the pod's new ones), again on every
// delete, and once per claimant a promotion scans — so the allocation it used
// to make unconditionally was paid by every reader waiting on that lock. The
// returned slice may therefore ALIAS the pod's own PodIPs and is read-only to
// callers; podAddresses honours that, and nothing else writes to it.
func rawIPs(pod kubemeta.Pod) []string {
	ips := pod.PodIPs
	if len(ips) == 0 && pod.PodIP != "" {
		ips = []string{pod.PodIP}
	}
	for i, ip := range ips {
		c := peerip.Canonical(ip)
		if c == ip {
			continue
		}
		out := make([]string, len(ips))
		copy(out, ips[:i])
		out[i] = c
		for j := i + 1; j < len(ips); j++ {
			out[j] = peerip.Canonical(ips[j])
		}
		return out
	}
	return ips
}

// podAddresses returns the addresses a pod may legitimately be reached at, or
// nil when it claims none (hostNetwork, finished).
func podAddresses(pod kubemeta.Pod) []string {
	// The spec flag is the authoritative signal; the IP comparison stays as a
	// backstop for records converted before the field existed.
	if pod.HostNetwork || kubemeta.FinishedPhase(pod.Phase) {
		return nil
	}
	ips := rawIPs(pod)
	// The host address in the same form the pod addresses are keyed in, or the
	// comparison below misses whenever the two are spelled differently.
	host := peerip.Canonical(pod.HostIP)
	// Nothing to filter is the case every ordinary pod takes, and it returns
	// rawIPs' slice rather than a copy of it (see rawIPs on why the store's
	// write lock makes that worth the branch). The copy is built only from the
	// first address that has to go.
	for i, ip := range ips {
		// A hostNetwork pod whose status.hostIP has not been populated yet
		// would otherwise claim the node address.
		if ip != "" && ip != host {
			continue
		}
		out := make([]string, 0, len(ips)-1)
		out = append(out, ips[:i]...)
		for _, ip := range ips[i+1:] {
			if ip != "" && ip != host {
				out = append(out, ip)
			}
		}
		return out
	}
	return ips
}

// recordAddresses returns every address a record ever claimed, including for a
// pod that has since gone finished or hostNetwork (podAddresses returns none
// for those, but their claims still have to be cleaned up).
func recordAddresses(rec *record) []string {
	return rawIPs(rec.pod)
}

// releaseIPLocked drops rec's claim on one address and promotes a survivor.
func (s *Store) releaseIPLocked(rec *record, ip string) {
	s.dropClaimantLocked(ip, rec)
	if s.byPodIP[ip] == rec {
		delete(s.byPodIP, ip)
		// A live pod shadowed on that address by the recycle race must be
		// promoted, or it stays unresolvable until its own next real upsert.
		s.promoteIPClaimantLocked(ip, rec)
	}
}

// claimOneIPLocked applies the claim rules for ONE of a pod's addresses.
//
// ACQUISITION ORDER IS THE WHOLE RULE, and the terminating bit deliberately
// decides nothing on its own. The rule that used to sit above it — "a draining
// pod yields to a live incumbent" — is entirely SUBSUMED by ipSeq whenever its
// own justification holds: a pod that keeps reporting an address "the CNI has
// already handed to someone else" is by construction the EARLIER acquirer, so
// it loses on ipSeq without any help. The only shapes in which the terminating
// arms decided anything were the ones their justification does NOT describe,
// and there they INVERTED the ordering: a live pod that merely re-asserted an
// address it acquired FIRST took it back from the pod that acquired it later
// and is now draining — which is not a recycle at all, since an address is not
// released until the sandbox is torn down. Any status churn on the stale pod
// (a node-lifecycle condition on a NotReady node, a resurrect after DeletePod)
// fired it, /v1/pod-ips, /v1/self and every agent peer-IP fallback then carried
// the stale pod's identity for the drainer's remaining traffic, and the flip
// did not heal when the drainer was finally deleted (releaseIPLocked promotes
// only when the leaver still HELD the address) — it stood until some new pod
// genuinely acquired the address. It was also silent: noteContested excludes
// every claim in which either side is terminating.
//
// The ordinary hand-off still happens, one event later and on real evidence:
// the drainer's deletion (or its transition to a finished phase) releases the
// address and promoteIPClaimantLocked hands it to the best surviving claimant.
//
// THE ONE PLACE ipSeq IS NOT EVIDENCE is a first sighting, and noEvidence is
// how the caller says so. ipSeq orders the transitions this store OBSERVED, and
// on the informer's initial LIST — every restart of this process — every pod
// is a first sighting, delivered in list (namespace/name) order rather than in
// the order the addresses were handed out. For two LIVE claimants that is the
// recycle race noteContested already counts. For a pod first seen ALREADY
// DRAINING beside a live claimant it is worse: the "earlier acquirer by
// construction" premise above does not hold, so which of the two held the
// address came down to their names, silently (noteContested skips every
// terminating pair), until the drainer was deleted — its grace period, or
// indefinitely for a pod stuck Terminating on a lost node. Such a claim
// therefore takes NO sequence (the record keeps ipSeq 0): it holds an address
// nobody else claims, never displaces a claimant with real evidence, and any
// later acquisition beats it. Where the first-seen drainer really WAS the later
// acquirer (the live claimant being a stale pod on a lost node that still
// reports the address) this resolves toward the pod that stays rather than the
// one leaving, which is the ambiguity a restart genuinely cannot settle — and
// either way the answer no longer depends on the two pods' names. It is per
// UPSERT rather than per address, since
// ipSeq is per record and bumping it on one address of a dual-stack pod would
// leak the value back onto the other.
//
// What ipSeq CANNOT tell apart is a re-acquisition from a return. "Already
// held" means present in oldIPs, the addresses the record reported last, so a
// pod that stops reporting an address and later reports it again — a podIP
// blip — or whose record was dropped outright and then resurrected (DeletePod
// with -cache-ttl <= 0 keeps no tombstone, so the resurrect has no oldIPs at
// all) is indistinguishable here from a pod that acquired the address anew, and
// it wins on ipSeq. That is the right answer when the sandbox really was
// re-created onto the same address, and nothing in a pod's status separates the
// two; TestAnAddressReportedAgainIsANewAcquisition pins the choice so changing
// it is deliberate.
//
// It deliberately takes no kubemeta.Pod: eligibility and precedence come from
// RECORD state the caller has already established (rec.ipSeq), not from the pod
// value. Passing the pod invited a later edit to re-derive it here and bypass
// the ipSeq ordering.
func (s *Store) claimOneIPLocked(rec *record, ip string, oldIPs []string, noEvidence bool) {
	s.addClaimantLocked(ip, rec)
	if !noEvidence && !slices.Contains(oldIPs, ip) {
		// This pod ACQUIRED the address now. The sequence orders genuine
		// acquisitions so a later one beats an earlier one below.
		s.ipSeq++
		rec.ipSeq = s.ipSeq
	}
	cur := s.byPodIP[ip]
	switch {
	case cur == nil || cur == rec:
		s.byPodIP[ip] = rec
	case beatsClaimant(cur, rec):
		// Last acquisition wins — including a late-scheduled older pod
		// legitimately taking a freed address, and a live pod taking over from
		// a drainer that acquired the address before it. noteContested skips
		// the latter: a hand-off from a terminating holder is the ordinary way
		// an address changes hands, not a window in which a lookup was wrong.
		s.noteContested(rec, cur)
		s.byPodIP[ip] = rec
	default:
		// rec is merely RE-ASSERTING an address it already held while a
		// later pod legitimately took it (or is a first sighting carrying no
		// acquisition evidence, above). Plain last-write-wins let any
		// unrelated update to a stale pod (a node-lifecycle condition on a
		// NotReady node, a resurrect after DeletePod while its tombstone is
		// retained, -cache-ttl > 0) steal the mapping from the later acquirer
		// and mis-attribute every peer-IP lookup until that pod finally went
		// away. A podIP BLIP is not protected here and is not meant to be: an
		// address that left the status and came back re-enters as a fresh
		// acquisition above, and takes the address — see the doc comment.
		s.noteContested(rec, cur)
	}
}

// promoteIPClaimantLocked re-points byPodIP[ip] at a surviving eligible pod
// after the current claimant released or lost the IP. Eligibility mirrors
// claimPodIPLocked, through the same podAddresses helper: live (not
// tombstoned), running-phase, non-hostNetwork, and holding ip among
// status.podIPs — the SECONDARY address of a dual-stack pod included. The LATER
// acquirer wins (same precedence the claim path applies, and for the same
// reason: a drainer has not released its address until its sandbox is torn
// down, which is the event that brings the promotion round again).
//
// skip is the record that just gave the IP up and must never win it back. It is
// this function's OWN precondition, not a patch for its caller: the scan is over
// ipClaimants[ip], and the one caller (releaseIPLocked) drops rec from that map
// BEFORE calling — so today the branch is never taken, and a test proving it is
// unreachable would prove nothing about whether it may be deleted.
//
// It stays because the alternative is a promotion whose correctness rests on
// call ORDER, and the outcome of getting that order wrong is silent. None of
// this scan's other filters would catch the releaser: DeletePod stamps expireAt
// (the tombstone marker filtered on above) only AFTER the promotion runs, and
// with -cache-ttl 0 removes the record instead of stamping it at all, so the pod
// being deleted still reads as live here. It would take its own address back
// with DeletedAt unset and serve a DELETED pod from GET /v1/pod-ips forever
// (sweep never revisits byPodIP), leaking one entry per deleted pod and — when a
// live pod holds the recycled IP — stealing the mapping from the real owner.
// TestPromotionNeverReturnsTheAddressToTheRecordThatReleasedIt calls this
// function directly, with the releaser still in the claimant set, so the
// exclusion is pinned as a contract rather than as unreachable code.
func (s *Store) promoteIPClaimantLocked(ip string, skip *record) {
	var pick *record
	for _, r := range s.ipClaimants[ip] {
		if r == skip { // released the IP; never a candidate to re-take it
			continue
		}
		if !r.expireAt.IsZero() { // tombstoned: not live
			continue
		}
		// Eligibility through the SAME helper the claim path uses: it returns
		// the addresses a pod may legitimately hold and nothing for a
		// hostNetwork or finished one, so this subsumes the four separate
		// checks that stood here. They compared the single p.PodIP, which
		// meant a dual-stack claimant could never be promoted onto its
		// SECONDARY address — the address then resolved to nothing at all.
		if !slices.Contains(podAddresses(r.pod), ip) {
			continue
		}
		if pick == nil || beatsClaimant(pick, r) {
			pick = r
		}
	}
	if pick != nil {
		s.byPodIP[ip] = pick
	}
}

// beatsClaimant reports whether candidate should displace cur for an address.
// It is the ONE precedence rule for the pod-IP index, called by both paths that
// decide a holder — the claim path (claimOneIPLocked) and promotion
// (promoteIPClaimantLocked) — so the two cannot drift: the LATER acquisition
// wins, and the terminating bit decides nothing. Preferring a live claimant on
// either path would promote a pod whose claim the other had already ruled
// stale, i.e. reintroduce on one path the outcome ipSeq exists to prevent.
// Promotion once had no ipSeq comparison at all, and the winner between two
// claimants was map-iteration random.
func beatsClaimant(cur, candidate *record) bool {
	return candidate.ipSeq > cur.ipSeq
}

// addClaimantLocked records that rec currently reports ip.
func (s *Store) addClaimantLocked(ip string, rec *record) {
	m := s.ipClaimants[ip]
	if m == nil {
		m = make(map[string]*record, 1)
		s.ipClaimants[ip] = m
	}
	m[rec.pod.UID] = rec
}

// dropClaimantLocked forgets rec's claim on ip, removing the address's entry
// once nobody reports it (the map must not grow by one key per recycled IP).
func (s *Store) dropClaimantLocked(ip string, rec *record) {
	m := s.ipClaimants[ip]
	if m == nil {
		return
	}
	if cur, ok := m[rec.pod.UID]; ok && cur == rec {
		delete(m, rec.pod.UID)
	}
	if len(m) == 0 {
		delete(s.ipClaimants, ip)
	}
}

// noteContested records that one address was claimed by two records that are
// both LIVE, whichever of them the precedence rules picked. It is the only
// shape of the recycle race in which a peer-IP lookup could legitimately have
// answered with the wrong pod, which is why the terminating hand-offs — the
// ordinary way an address is released — are excluded.
func (s *Store) noteContested(rec, cur *record) {
	if rec.terminating || cur.terminating {
		return
	}
	s.ipContested.Add(1)
}

// ContestedPodIPs counts pod-IP claims decided between two live pods (see the
// ipContested field). Published through obs.RegisterStoreAnomalies.
func (s *Store) ContestedPodIPs() int64 { return s.ipContested.Load() }
