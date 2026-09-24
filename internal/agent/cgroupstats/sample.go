package cgroupstats

// The sample path: the per-interval sweep over every tracked container, three
// preads each, allocation-free (TestSampleAllocationBudget).

import (
	"context"
	"path/filepath"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// sampleLoop is the reader, and it does NOTHING that can block: every read is
// a pread on a descriptor this process already holds. See the Sampler doc for
// why discovery is not here any more.
func (s *Sampler) sampleLoop(ctx context.Context) {
	sampleTick := time.NewTicker(s.interval)
	defer sampleTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sampleTick.C:
			s.sample()
		}
	}
}

// sample reads every tracked container once. It runs on the sampler goroutine
// and is allocation-free (TestSampleAllocationBudget); keep it that way — it
// runs once per second per container on the process that also tails every log
// file on the node.
//
// Nothing here may BLOCK. Every read is a pread on a descriptor this process
// already holds, and that is the property that lets the interval mean what it
// says; see the Sampler doc for what sharing this goroutine with discovery
// cost the numbers.
func (s *Sampler) sample() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.tracked {
		if !c.open {
			continue // a vanished cgroup, waiting for its final flush
		}
		s.sampleOne(c)
	}
}

func (s *Sampler) sampleOne(c *container) {
	obs.CgroupSamples.Inc()
	// A sample was attempted, which is what makes a window that produced
	// nothing evidence of an unreadable container rather than of a sampler that
	// was not running (see endWindow).
	c.tried = true

	if usec, err := readField(c.fds.cpuStat, s.buf, keyUsageUsec); err != nil {
		s.c.readCPUStat.Inc()
		s.warnRead(c, fileCPUStat, err)
	} else {
		c.readOK = true
		// The clock is read HERE, per container, right after the read that
		// produced the counter — not once per sweep. A sweep timestamp is the
		// interval between sweep STARTS while each container is read at its own
		// varying offset inside the sweep, so the jitter between two offsets
		// lands in the divisor: measured, a dead-flat container reported a
		// stddev of up to 0.012 cores and an inflated max out of nothing but
		// that. Per-container timing cancels it, because both ends of the
		// interval carry the same offset.
		now := s.now()
		// rebase says this reading becomes the next interval's baseline. It is
		// false in exactly one case, the short interval below, and that case is
		// why the assignment is not unconditional any more.
		rebase := true
		switch {
		case !c.havePrev:
			// First reading of this container: no interval, so no rate.
		case usec < c.prevUsec:
			// A cumulative counter went backwards. On cgroup v2 that means the
			// cgroup was replaced under the same path (a container restart
			// keeping its id is impossible, but a re-created cgroup is not), and
			// the difference would be a large NEGATIVE rate rendered as a huge
			// positive one by the unsigned subtraction. The interval is dropped
			// and the new value becomes the baseline.
			obs.CgroupCounterResets.Inc()
		case now.Sub(c.prevAt) < s.minElapsed:
			// Too short an interval to divide by. usage_usec advances in the
			// scheduler's accounting quanta, so over a few milliseconds the
			// quotient is dominated by where the quanta happened to land — an
			// arithmetically correct rate for the interval measured, and
			// therefore indistinguishable downstream from the real burst _max
			// exists to preserve. It happens when the sampler was blocked and
			// the ticker is catching up (see the Sampler doc for the measured
			// spread: a 0.5-core container's 5 ms catch-up pair reads 0.00 to
			// 0.97 cores, half the draws above its true steady-state max).
			//
			// WHAT IS AND IS NOT LOST, stated precisely because the loose
			// version of this sentence ("nothing is lost") was true only of
			// the CPU-SECONDS and this pipeline's product is the DISTRIBUTION:
			//
			//   - the CPU TIME is not lost. The baseline is KEPT (rebase =
			//     false) and the counter is cumulative, so the sliver's
			//     microseconds are charged to the NEXT interval rather than to
			//     one of their own, and every microsecond cpu.stat reported
			//     still lands in exactly one interval —
			//     TestAShortIntervalIsDeferredNotDiscarded pins that sum.
			//   - the window's MEAN is NOT preserved, and the sentence that
			//     stood here ("the MEAN comes out the same to the last bit")
			//     was simply wrong, as well as contradicting the bullet below
			//     it: mean is the unweighted average of the PER-INTERVAL
			//     RATES, so folding two intervals into one removes a term AND
			//     lengthens the divisor of the term that survives. In that
			//     same test's fixture it reads (1 + 3/1.4 + 1)/3 = 1.38 cores
			//     where the four undeferred intervals would have averaged
			//     (1 + 5 + 1 + 1)/4 = 2.00 — which is the trade, not a defect:
			//     the 5.0 term is the stall artefact.
			//   - the window's SAMPLE is lost: it takes n-1 readings, so
			//     stddev, max and min are computed over one fewer point. A
			//     burst confined to the sliver is not erased — it lands in the
			//     next interval — but it is DILUTED over that longer interval,
			//     which is a smaller `_max` than a burst of the same size
			//     landing inside an ordinary interval.
			//
			// It is not counted, and the argument for that is not that it is
			// free: it is that the loss is already REPORTED, per container and
			// per window, by container_cpu_usage_samples — which exists for
			// exactly this class of question and drops by one for every skip
			// here. A fleet-wide counter would say the same thing less
			// precisely, and the alternative to skipping is worse in the
			// direction that matters: an inflated `_max` is a wrong number
			// where a missing sample is a smaller n, and only the first is
			// indistinguishable from the burst the gauge exists to preserve.
			//
			// Measured on the shipped arrangement (200 real cgroup v2 scopes,
			// discovery and export on their own goroutines) it fired ZERO
			// times in 23,600 rate readings at the 1s default and zero in
			// 239,600 at the 100ms floor: on a healthy node this arm is inert,
			// and it arms only once something has already stalled the sweep.
			rebase = false
		default:
			if el := now.Sub(c.prevAt).Seconds(); el > 0 {
				// usage_usec is MICROseconds of CPU time; divided by the wall
				// seconds it accrued over, that is CPU cores.
				c.cpu.add(float64(usec-c.prevUsec) / 1e6 / el)
			}
		}
		if rebase {
			c.prevUsec, c.prevAt, c.havePrev = usec, now, true
		}
	}

	// The memory working set is a LEVEL, not a rate, so it has no divisor for a
	// short interval to corrupt: two readings close together are two honest
	// readings of what the container held, and the guard above deliberately
	// does not apply here.
	cur, curErr := readValue(c.fds.memCurrent, s.buf)
	if curErr != nil {
		s.c.readMemCurrent.Inc()
		s.warnRead(c, fileMemCurrent, curErr)
	} else {
		c.readOK = true
	}
	inactive, inactErr := readField(c.fds.memStat, s.buf, keyInactiveFile)
	if inactErr != nil {
		s.c.readMemStat.Inc()
		s.warnRead(c, fileMemStat, inactErr)
	} else {
		c.readOK = true
	}
	if curErr == nil && inactErr == nil {
		// cadvisor's own definition of the working set, matched exactly:
		// container_memory_working_set_bytes is memory.current minus the
		// inactive file cache. A different definition here would make _max and
		// _min incomparable with the series they annotate, which is the whole
		// reason this package borrows cadvisor's naming and resource identity.
		var ws float64
		if cur > inactive {
			ws = float64(cur - inactive)
		}
		c.mem.add(ws)
	}
}

// warnRead names one failing cgroup file per readWarnEvery. It is on the
// error arm only, so the healthy sample path never reaches time.Now.
//
// A container torn down between two discovery passes fails every read until
// the next pass drops it, which is a normal few seconds of noise on a node with
// churn — hence the throttle rather than a line per failure, and hence a
// warning that says so.
func (s *Sampler) warnRead(c *container, file string, err error) {
	if !s.readWarn.Allow(readWarnEvery) {
		return
	}
	s.log.Warn("cgroup read failed (throttled; a container removed between discovery passes fails every read until the next one)",
		"path", filepath.Join(c.dir, file), "error", err)
}
