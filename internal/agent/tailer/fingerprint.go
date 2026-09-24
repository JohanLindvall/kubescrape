package tailer

// Content identity: the head fingerprint that, with the inode, tells one
// incarnation of a path from another (inode reuse, copytruncate), and its
// extension as a freshly opened file grows.

import (
	"errors"
	"hash/fnv"
	"io"
	"os"
)

// fingerprint identifies file content: an FNV-1a hash of the first Len
// bytes. A zero Len means "not recorded" and matches anything.
type fingerprint struct {
	Len  int64
	Hash uint64
}

// fingerprintChunk is the stack buffer computeFingerprint hashes through.
// FNV-1a streams, so a head of any length hashed in pieces yields exactly the
// hash of the whole head: persisted fingerprints (fpLen/fpHash) are unchanged.
const fingerprintChunk = 4096

// fingerprintReadHook, when non-nil, is called once per head read — a TEST
// seam (TestRotationClassificationReadsTheFingerprintOnce counts reads with
// it); nil in production.
var fingerprintReadHook func()

// computeFingerprint hashes the first n bytes of f (independent of the read
// offset).
//
// It takes the concrete *os.File and not an io.ReaderAt so the buffer can live
// on the STACK: a buffer handed to an interface method escapes. This runs on
// readFile's pre-read copytruncate re-verify — every read of every file whose
// mtime moved, i.e. every read of an active file — where a per-call
// make([]byte, n) was 1 KiB of garbage per read, 1 of a one-line read's 6
// allocations (TestFingerprintMatchesIsAllocationFree).
func computeFingerprint(f *os.File, n int64) (fingerprint, error) {
	if n <= 0 {
		return fingerprint{}, nil
	}
	if fingerprintReadHook != nil {
		fingerprintReadHook()
	}
	var buf [fingerprintChunk]byte
	h := fnv.New64a()
	var got int64
	for got < n {
		read, err := f.ReadAt(buf[:min(n-got, fingerprintChunk)], got)
		_, _ = h.Write(buf[:read])
		got += int64(read)
		if errors.Is(err, io.EOF) || (err == nil && read == 0) {
			break // a head shorter than n: fingerprint what is there
		}
		if err != nil {
			return fingerprint{}, err
		}
	}
	return fingerprint{Len: got, Hash: h.Sum64()}, nil
}

// identityChanged reports whether the file previously had a recorded identity
// and the inode or head content at hand no longer matches it — the path (or
// the held fd) now names a DIFFERENT incarnation. Shared by ensureOpen and
// openArchive, whose replaced-file decisions must agree.
func (f *file) identityChanged(inode uint64, fh *os.File) bool {
	return f.inode != 0 && (f.inode != inode || !f.fp.matches(fh))
}

// matches reports whether the file still begins with the fingerprinted
// content.
func (fp fingerprint) matches(f *os.File) bool {
	if fp.Len == 0 {
		return true
	}
	cur, err := computeFingerprint(f, fp.Len)
	return err == nil && cur == fp
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
