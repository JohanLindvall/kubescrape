package tailer

// The incremental read path for live (non-archive) files: metadata
// resolution, the per-sweep read loop, open/identity verification, and
// fingerprint extension.

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/metaclient"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// resolveMetadata builds the file's resource attributes. Plain files resolve
// immediately from the source's static attributes plus node metadata;
// containerd files fetch pod metadata from the service (backing off between
// attempts), and are not consumed until it is available — the data waits on
// disk, nothing is lost.
// metadata resolution backoff bounds: the first retry is quick (a container
// genuinely racing the API server resolves within a second or two), the cap
// keeps a permanently unresolvable file down to one blocking call a minute.
const (
	minMetaBackoff = 2 * time.Second
	maxMetaBackoff = time.Minute
)

func nextMetaBackoff(cur time.Duration) time.Duration {
	if cur <= 0 {
		return minMetaBackoff
	}
	if cur >= maxMetaBackoff {
		return maxMetaBackoff
	}
	return min(cur*2, maxMetaBackoff)
}

// jitterMetaBackoff spreads the retry over [d, 1.25d).
//
// Every file on a node is resolved by one goroutine against one service, so a
// metadata-service rollout puts every file on the SAME schedule: they all
// fail together, double together and hit the recovered service together — a
// synchronised burst from every node in the fleet at once, which is how a
// rollout turns into a second outage. The skew is small (the cap stays ~1m)
// but it is enough to decorrelate.
func jitterMetaBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(int64(d/4)+1))
}

func (t *Tailer) resolveMetadata(ctx context.Context, f *file) bool {
	if !f.source.containerd {
		return t.resolvePlain(f)
	}
	if time.Now().Before(f.nextMetaTry) {
		return false
	}
	md, err := t.cfg.Metadata.Container(ctx, f.containerID, t.cfg.MetadataWait)
	if err != nil {
		// EXPONENTIAL backoff. The lookup deliberately BLOCKS server-side for
		// the whole -metadata-wait when the container is unknown, and it runs
		// on the single sweep goroutine that serves every file on the node, so
		// a flat retry shorter than the cost of the sweep it spaces out lets a
		// few permanently unresolvable files (a deleted pod whose tombstone has
		// expired) monopolise the tailer: nothing is read, no rotation is
		// noticed, and a file rotating twice inside that window loses the
		// middle incarnation.
		f.metaBackoff = nextMetaBackoff(f.metaBackoff)
		f.nextMetaTry = time.Now().Add(jitterMetaBackoff(f.metaBackoff))
		if metaclient.IsNotFound(err) {
			t.log.Debug("container metadata not found yet", "id", f.containerID)
		} else {
			t.log.Warn("fetching container metadata", "id", f.containerID, "error", err)
		}
		return false
	}
	f.metaBackoff = 0
	res := pcommon.NewResource()
	actx := attrs.Context{Pod: &md.Pod, Container: &md.Container}
	if t.cfg.NodeInfo != nil {
		actx.Node = t.cfg.NodeInfo()
	}
	t.cfg.Attrs.Build(res, actx)
	f.resource = res
	f.resolved = true
	if !f.source.wantLabels(md.Pod.Labels) {
		// The source selects pods by label; this one does not match. Labels are
		// only known now, so the file is tracked — but no data is ever read
		// from it, unlike a logs.rules drop which pays read+parse+enrich first.
		f.excluded = true
		t.log.Debug("pod does not match the source selector; not collecting",
			"path", f.path, "source", f.source.name)
		return true
	}
	t.applyPodConfig(f, md.Pod.Annotations)
	return true
}

// resolvePlain builds a non-containerd file's resource: node attributes from
// the builder plus the source's configured static attributes (which win). A
// source without an explicit service.name defaults it to the source name.
func (t *Tailer) resolvePlain(f *file) bool {
	res := pcommon.NewResource()
	actx := attrs.Context{}
	if t.cfg.NodeInfo != nil {
		actx.Node = t.cfg.NodeInfo()
	}
	t.cfg.Attrs.Build(res, actx)
	a := res.Attributes()
	if _, ok := f.source.attributes["service.name"]; !ok && f.source.name != "" {
		if _, set := a.Get("service.name"); !set {
			a.PutStr("service.name", f.source.name)
		}
	}
	for k, v := range f.source.attributes {
		a.PutStr(k, v)
	}
	f.resource = res
	f.resolved = true
	return true
}

// readFile ingests up to MaxBytesPerSweep appended bytes and detects
// rotation.
func (t *Tailer) readFile(ctx context.Context, f *file) error {
	if f.compressed {
		return t.readArchive(ctx, f)
	}
	if err := t.ensureOpen(f); err != nil {
		return err
	}
	// A group straddled a rename rotation and the pipeline was since discarded
	// (rewind or restart): re-read the rotated-away prefix before the new inode
	// so the group reconstructs.
	t.feedSegments(ctx, f)

	// Copytruncate whose replacement content is LONGER than our read offset:
	// the post-read check below cannot see it (bytes come back from the stale
	// offset, so read > 0 and its `read == 0` guard never fires) and we would
	// resume mid-way into the new file, silently skipping its prefix — and
	// splitting a line. The head fingerprint is the only witness, so re-verify
	// it here, BEFORE consuming anything, whenever the file changed on disk
	// since our last read. Costs one stat plus a fingerprint hash per changed
	// file per sweep.
	if f.readPos > 0 {
		if st, err := os.Stat(f.path); err == nil &&
			inodeOf(st) == f.inode && st.Size() >= f.readPos &&
			!st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f) {
			t.reopen(ctx, f, false)
			f.lastMod = st.ModTime()
			if err := t.ensureOpen(f); err != nil {
				return err
			}
		}
	}

	// A paused (rate-limited) file first retries its retained pending bytes;
	// reading resumes only once they drain.
	if f.limited {
		t.consume(ctx, f, false)
	}
	budget := t.cfg.MaxBytesPerSweep
	buf := t.scratch()
	read := 0
	for budget > 0 && !f.limited {
		limit := min(len(buf), budget)
		n, err := f.f.Read(buf[:limit])
		if n > 0 {
			budget -= n
			read += n
			t.ingestChunk(ctx, f, buf[:n], false)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
	}

	// A first read on a file opened at size 0 (a fresh container log) leaves
	// fp.Len == 0, which matches ANYTHING — extend as soon as content exists,
	// not only on the checkpoint cadence (which never runs without a store).
	if read > 0 {
		t.extendFingerprint(f)
	}

	// Rotation/truncation detection.
	st, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	if !t.handleRotation(ctx, f, st, read) {
		return nil // aborted rotation drain: retry next sweep, lastMod unstamped
	}
	f.lastMod = st.ModTime()
	return nil
}

// handleRotation classifies what happened to the file on disk since the last
// read — rename rotation (new inode at the path), in-place truncation, or a
// same-size copytruncate only the fingerprint can witness — and runs the
// matching recovery (no-op when the identity is unchanged). It reports false
// when a rename rotation's drain aborted (mid-drain flush failure): the
// caller must leave lastMod unstamped so the rotation is re-detected and
// retried next sweep.
func (t *Tailer) handleRotation(ctx context.Context, f *file, st os.FileInfo, read int) bool {
	switch {
	case inodeOf(st) != f.inode:
		// Rename rotation: the path names a new file. Drain what the old
		// writer appended after our last read, then switch — carrying a
		// straddling multi-line group across the boundary. An aborted drain
		// (mid-drain flush failure rewound this fd) must NOT proceed to
		// reopen — the old inode is not fully consumed and the segment would
		// exclude its unread tail; stay on the old inode and retry the whole
		// rotation next sweep, with the sweep cadence as the backoff.
		if !t.drainFile(ctx, f) {
			f.dirty = true
			return false
		}
		t.reopen(ctx, f, true)
		// Open the NEW incarnation now, not on the next sweep. reopen clears
		// f.f/f.inode/f.fp, so until the file is opened again it has no fd and
		// no identity: a SECOND rotation inside that window finds nothing to
		// drain and records no segment, and the whole inode is unreachable
		// forever — silently and uncounted. The window is a full sweep, and it
		// widens to seconds whenever the sweep goroutine is blocked in a
		// failing export, which is exactly when rotations pile up. The
		// copytruncate branch above already re-opens for the same reason.
		if err := t.ensureOpen(f); err != nil {
			t.log.Debug("opening rotated-in file", "path", f.path, "error", err)
		}
	case st.Size() < f.readPos:
		// In-place truncation: the unread tail is gone; restart at zero.
		// (Draining would read the replacement content mid-stream.)
		t.reopen(ctx, f, false)
	case read == 0 && !st.ModTime().Equal(f.lastMod) && !f.fp.matches(f.f):
		// The file changed without yielding new bytes past our offset and
		// its head no longer matches: truncated and rewritten to a size at
		// or beyond our position (same-size copytruncate). Restart.
		t.reopen(ctx, f, false)
	}
	return true
}

// ensureOpen opens the file at the committed offset on first use. The
// offset is only honored when the file's identity (inode and fingerprint)
// still matches; otherwise the path names a different file and reading
// starts from the top.
func (t *Tailer) ensureOpen(f *file) error {
	if f.f != nil {
		return nil
	}
	fh, err := os.Open(f.path)
	if err != nil {
		return err
	}
	st, err := fh.Stat()
	if err != nil {
		_ = fh.Close()
		return err
	}
	inode := inodeOf(st)
	start := f.committed
	// A DIFFERENT file now lives at this path: -logs-idle-close released the
	// fd and the runtime rotated a replacement in while we held none.
	replaced := f.inode != 0 && (f.inode != inode || !f.fp.matches(fh))
	if replaced {
		start = 0
	}
	if start > st.Size() {
		start = 0
	}
	if _, err := fh.Seek(start, 0); err != nil {
		_ = fh.Close()
		return err
	}
	fp, err := computeFingerprint(fh, min(int64(t.cfg.FingerprintBytes), st.Size()))
	if err != nil {
		_ = fh.Close()
		return err
	}
	f.f = fh
	f.inode = inode
	f.fp = fp
	if replaced {
		// A new incarnation, so take the canonical path rather than resetting
		// byte positions inline. Keeping the OLD tail id attributed the new
		// inode's bytes to the previous incarnation's segment, and a withheld
		// exportedHigh from that incarnation was later re-offered and applied
		// here — advancing `committed` past bytes this file never read. The
		// inline reset also skipped restartAt, so a rate-limit pause or an
		// oversized-line discard window survived into a file that has nothing
		// to do with them.
		f.exportedHigh = pos{}
		f.newTail()
		t.newPipeline(f) // fresh stages; reset() re-arms the segment replay
	}
	f.restartAt(start)
	f.committed = start
	t.watchTarget(f)
	return nil
}

// extendFingerprint grows a short fingerprint once the file has grown past
// the initial hash length, up to the configured size — but ONLY while the
// head we already hashed is still there. Re-hashing unconditionally adopts
// whatever the head happens to be now, so a copytruncate landing between a
// read and this call would rewrite fp to the REPLACEMENT's head and blind
// the rotation guards (which compare against fp) — silently, and for every
// file below FingerprintBytes, i.e. every quiet container.
//
// Called from saveCheckpoints AND from readFile after a successful read:
// without the read-path call, a deployment with no checkpoint store never
// extends at all, so a file first opened at size 0 keeps the
// matches-anything empty fingerprint forever and every fp-based rotation
// guard is permanently blind for it.
func (t *Tailer) extendFingerprint(f *file) {
	if f.f == nil || t.cfg.FingerprintBytes <= 0 || f.fp.Len >= int64(t.cfg.FingerprintBytes) || !f.fp.matches(f.f) {
		return
	}
	if st, err := f.f.Stat(); err == nil && st.Size() > f.fp.Len {
		if fp, err := computeFingerprint(f.f, min(int64(t.cfg.FingerprintBytes), st.Size())); err == nil {
			f.fp = fp
		}
	}
}
