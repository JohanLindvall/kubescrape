//go:build linux

package cgroupstats

// Listing a cgroup directory's SUBDIRECTORIES without materialising its files.
//
// This is the discovery pass's whole cost, and the ratio is what makes it worth
// a raw getdents64 loop rather than os.ReadDir: a cgroup v2 directory holds
// ~45 control FILES (cpu.stat, memory.current, cgroup.procs, pids.max, …) and
// one or two child directories, and the walk wants nothing but the directories.
// os.ReadDir materialises a DirEntry and a name string for every entry, so a
// pass over a 200-container node allocated 21,950 objects and 1.44 MiB, of
// which 98% were the control files it discarded on the next line — and this
// file's walk over the same real 200-container hierarchy costs 1,642 objects
// and 243 KiB, thirteen times fewer (A/B, ten passes each). It was the single
// largest allocation source in the pipeline; the per-second sample sweep is
// allocation-free by construction. Every discovery interval, forever, and
// -cgroup-stats-discover-interval exists to let an operator run it four times
// more often.
//
// getdents64 hands back the entry TYPE beside the name, so the filter happens
// before anything is allocated: the same walk now costs a slice and one string
// per SUBDIRECTORY. The buffer is the scan's, reused across every directory of
// a pass.
//
// The layout parsed here is linux_dirent64, which is the same on every Linux
// architecture (that is the point of the 64 suffix — unlike the legacy
// getdents, it does not vary with the word size):
//
//	u64 d_ino; s64 d_off; u16 d_reclen; u8 d_type; char d_name[];
//
// syscall.Dirent declares exactly those fields in that order on every linux
// GOARCH. What pins it is TestSubdirsMatchesReadDir, which compares this
// against os.ReadDir over real directories — a fabricated one holding every
// entry shape a walk can meet, plus the live /sys/fs/cgroup and /proc — and
// which therefore runs on whatever architecture the code is built for rather
// than asserting something about the one it was written on.

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
)

// Offsets into one linux_dirent64 record. See the file comment.
const (
	direntReclenOff = 16
	direntTypeOff   = 18
	direntNameOff   = 19
	// direntHeaderBytes is the fixed part; a record is never shorter.
	direntHeaderBytes = direntNameOff
)

// dirBufBytes is the getdents64 buffer. One page holds a cgroup directory's
// ~45 records (each ~24-40 bytes) in a single syscall, which is what the loop
// below is sized for; a bigger buffer would only matter for directories this
// walk never reads.
const dirBufBytes = 4096

// subdirs returns the names of dir's immediate subdirectories, using buf as
// scratch (it is overwritten and never retained).
//
// A symlink is NOT a subdirectory here, matching os.ReadDir's DirEntry.IsDir
// (which reports the entry's own type and does not follow links) — the cgroup
// hierarchy contains none, and quietly following one would let a walk out of
// the tree it is budgeted for.
func subdirs(dir string, buf []byte) ([]string, error) {
	fd, err := openDirRO(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = syscall.Close(fd) }()

	var out []string
	for {
		n, err := readDirent(fd, buf)
		if err != nil {
			return nil, &os.PathError{Op: "getdents", Path: dir, Err: err}
		}
		if n <= 0 {
			return out, nil
		}
		out, err = appendDirents(out, buf[:n], dir)
		if err != nil {
			return nil, &os.PathError{Op: "getdents", Path: dir, Err: err}
		}
	}
}

// appendDirents parses one getdents64 buffer, appending the subdirectory names.
func appendDirents(out []string, b []byte, dir string) ([]string, error) {
	for off := 0; off < len(b); {
		if off+direntHeaderBytes > len(b) {
			return out, syscall.EIO
		}
		reclen := int(binary.NativeEndian.Uint16(b[off+direntReclenOff:]))
		// A record shorter than its header, or running past what the kernel
		// said it wrote, means this is not a dirent stream at all. Refusing is
		// the only safe move: continuing would read the rest of the buffer at
		// an arbitrary offset and invent entries.
		if reclen < direntHeaderBytes || off+reclen > len(b) {
			return out, syscall.EIO
		}
		typ := b[off+direntTypeOff]
		name := b[off+direntNameOff : off+reclen]
		if i := bytes.IndexByte(name, 0); i >= 0 {
			name = name[:i]
		}
		off += reclen
		// string(name) == "." does not allocate; the compiler compares bytes.
		if len(name) == 0 || string(name) == "." || string(name) == ".." {
			continue
		}
		switch typ {
		case syscall.DT_DIR:
		case syscall.DT_UNKNOWN:
			// Some filesystems do not fill d_type. cgroupfs does, so this arm
			// is unreachable on a real node — but a mis-pointed
			// -cgroup-stats-root can name any filesystem, and silently reading
			// "unknown" as "not a directory" would find no containers and
			// report no error, which is the one outcome this pipeline is built
			// never to produce quietly. os.ReadDir lstats in exactly this case.
			fi, err := os.Lstat(filepath.Join(dir, string(name)))
			if err != nil || !fi.IsDir() {
				continue
			}
		default:
			continue
		}
		out = append(out, string(name))
	}
	return out, nil
}

// openDirRO opens a directory for reading, retrying EINTR. O_DIRECTORY makes a
// non-directory an ENOTDIR at open rather than an EISDIR-shaped surprise in the
// getdents loop.
func openDirRO(dir string) (int, error) {
	for {
		fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return -1, &os.PathError{Op: "open", Path: dir, Err: err}
		}
		return fd, nil
	}
}

// readDirent is syscall.ReadDirent (getdents64) with EINTR retried.
func readDirent(fd int, buf []byte) (int, error) {
	for {
		n, err := syscall.ReadDirent(fd, buf)
		if err == syscall.EINTR {
			continue
		}
		if n < 0 {
			n = 0
		}
		return n, err
	}
}
