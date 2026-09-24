package cgroupstats

import (
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// counters are this pipeline's label bindings of the obs families, bound ONCE
// rather than per observation so the sample path's error arm costs an atomic
// add and not a label-vector lookup — the failing case is exactly the one that
// repeats every second.
//
// They live on the SAMPLER and are bound in New, not in a package-level var
// block, because BINDING A LABEL SET PUBLISHES ITS SERIES AT ZERO
// (internal/metrics' vec.with → series.materialize, deliberately: a bound
// value is a statement that this process can produce that outcome). A var
// block runs at package initialisation, which happens because
// cmd/kubescrape-agent IMPORTS this package — so every agent in the fleet
// published fifteen kubescrape_cgroup_* series reading 0 whether or not
// -cgroup-stats was on, which is precisely the ambiguity the pipeline's own
// gauge was built to avoid (obs.RegisterCgroupStats: "a published 0 always
// means enabled and finding nothing"). An operator alerting on
// kubescrape_cgroup_read_errors_total or graphing
// kubescrape_cgroup_windows_dropped_total could not tell a healthy sampler
// from an absent one.
//
// New binds them AFTER the cgroup-version check, so a node that refuses the
// pipeline (ErrUnsupportedNode) publishes nothing either.
type counters struct {
	readCPUStat, readMemCurrent, readMemStat *metrics.RegCounter

	unresolvedPending, unresolvedUnreachable *metrics.RegCounter
	unresolvedAbandoned, unresolvedExport    *metrics.RegCounter

	cappedTracked, cappedPending *metrics.RegCounter

	listErrRoot, listErrSubtree *metrics.RegCounter

	droppedUnresolved, droppedExport, droppedTooShort *metrics.RegCounter

	heldWindows, heldExpired *metrics.RegCounter
}

func newCounters() *counters {
	// The UNLABELED families of this pipeline, published at 0 by the same rule
	// and for the same reason. metrics.Registry.Counter deliberately publishes
	// nothing at REGISTRATION (obs registers both binaries' metrics at package
	// init, so a zero there would assert "this never happened" about a feature
	// the process does not contain), and there is no exported way to
	// materialise a scalar series other than adding zero to it — which for a
	// cumulative counter is exactly the statement wanted: this outcome exists
	// here and has occurred no times. Without it, four of these five are rare
	// by design (an open error, a counter reset, a truncated scan, a
	// retirement), so "absent" and "healthy" looked identical on the one
	// pipeline that argues hardest that they must not.
	for _, c := range []*metrics.RegCounter{
		obs.CgroupSamples, obs.CgroupOpenErrors, obs.CgroupCounterResets,
		obs.CgroupScanTruncated, obs.CgroupContainersRetired,
	} {
		c.Add(0)
	}
	return &counters{
		readCPUStat:    obs.CgroupReadErrors.WithLabelValues(fileCPUStat),
		readMemCurrent: obs.CgroupReadErrors.WithLabelValues(fileMemCurrent),
		readMemStat:    obs.CgroupReadErrors.WithLabelValues(fileMemStat),

		unresolvedPending:     obs.CgroupUnresolved.WithLabelValues("pending"),
		unresolvedUnreachable: obs.CgroupUnresolved.WithLabelValues("unreachable"),
		unresolvedAbandoned:   obs.CgroupUnresolved.WithLabelValues("abandoned"),
		unresolvedExport:      obs.CgroupUnresolved.WithLabelValues("export"),

		cappedTracked: obs.CgroupContainersCapped.WithLabelValues("tracked"),
		cappedPending: obs.CgroupContainersCapped.WithLabelValues("pending"),

		listErrRoot:    obs.CgroupDiscoveryErrors.WithLabelValues("root"),
		listErrSubtree: obs.CgroupDiscoveryErrors.WithLabelValues("subtree"),

		droppedUnresolved: obs.CgroupWindowsDropped.WithLabelValues("unresolved"),
		droppedExport:     obs.CgroupWindowsDropped.WithLabelValues("export_failed"),
		// too_short is the observable half of the short-lived-container blind
		// spot; see Sampler.finalWindowLocked.
		droppedTooShort: obs.CgroupWindowsDropped.WithLabelValues("too_short"),

		heldWindows: obs.CgroupHeldWindows.WithLabelValues("held"),
		heldExpired: obs.CgroupHeldWindows.WithLabelValues("expired"),
	}
}
