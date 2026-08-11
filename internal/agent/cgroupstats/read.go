package cgroupstats

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"syscall"
)

// The three cgroup v2 files one sample reads, and the two keys it needs out of
// them.
const (
	fileCPUStat    = "cpu.stat"
	fileMemCurrent = "memory.current"
	fileMemStat    = "memory.stat"

	// keyUsageUsec is cpu.stat's cumulative CPU time in MICROseconds. It is the
	// file's first line on every kernel that has the field, but it is looked up
	// by key rather than by position: the field list has grown over kernel
	// versions (core_sched.force_idle_usec, nr_bursts, burst_usec) and reading
	// "the first line" would be a silent misread the day it does not.
	keyUsageUsec = "usage_usec"
	// keyInactiveFile is memory.stat's inactive file cache, the term cadvisor
	// subtracts from memory.current to get the working set.
	keyInactiveFile = "inactive_file"
)

// readBufBytes is the shared scratch for every pread. One page covers cpu.stat
// (200 bytes) and memory.current (under 20) whole, and reaches inactive_file in
// memory.stat — which is ~1.4 KiB in total and carries the field around byte
// 360 — in the FIRST read.
//
// Stopping the SCAN at the key saves nothing and is not why the loop is shaped
// this way (this comment claimed otherwise, and a measurement disagreed): the
// cost of reading a cgroup file is the syscall, in which the kernel formats the
// whole seq_file up to the buffer size, and the memchr over the page that
// arrived is lost in the noise beside it. What the shape DOES buy is the only
// two things worth having here — one syscall for a file that fits in the
// buffer, and a bound: a file that does not hold the key within readBufBytes is
// a counted error rather than a second, third and fourth pread of a file whose
// size this process does not control.
const readBufBytes = 4096

var (
	errFieldMissing = errors.New("cgroupstats: field not present in the file")
	errFieldTooFar  = fmt.Errorf("cgroupstats: field not within the first %d bytes of the file", readBufBytes)
	errNotANumber   = errors.New("cgroupstats: value is not an unsigned integer")
	errValueTooLong = fmt.Errorf("cgroupstats: whole-file value not terminated within the first %d bytes", readBufBytes)
)

// cgroupFDs are one container's three open descriptors.
//
// They are held for the container's lifetime rather than opened per sample, and
// that is the whole reason the sample path can be allocation-free: opening by
// path costs a string join and a syscall.BytePtrFromString copy per read, which
// at three reads per container per second on a 200-container node is 600
// allocations a second on the process that also tails every log file on the
// node. Re-reading a held descriptor with pread(2) at offset 0 yields a FRESH
// snapshot — kernfs seq_files restart their iteration whenever the offset does
// not continue the previous read — so nothing has to be reopened or seeked.
//
// The cost is three descriptors per container, which is what maxContainers
// bounds.
type cgroupFDs struct {
	cpuStat    int
	memCurrent int
	memStat    int
}

// openCgroupFDs opens the three files of one container cgroup directory. A
// partial failure closes whatever was opened: a container missing any of the
// three cannot be sampled, and a half-open set would leak descriptors for the
// life of the process.
func openCgroupFDs(dir string) (cgroupFDs, error) {
	var fds cgroupFDs
	var err error
	if fds.cpuStat, err = openRO(filepath.Join(dir, fileCPUStat)); err != nil {
		return cgroupFDs{}, err
	}
	if fds.memCurrent, err = openRO(filepath.Join(dir, fileMemCurrent)); err != nil {
		_ = syscall.Close(fds.cpuStat)
		return cgroupFDs{}, err
	}
	if fds.memStat, err = openRO(filepath.Join(dir, fileMemStat)); err != nil {
		_ = syscall.Close(fds.cpuStat)
		_ = syscall.Close(fds.memCurrent)
		return cgroupFDs{}, err
	}
	return fds, nil
}

func (f cgroupFDs) close() {
	_ = syscall.Close(f.cpuStat)
	_ = syscall.Close(f.memCurrent)
	_ = syscall.Close(f.memStat)
}

// openRO opens one cgroup file read-only. The error is wrapped in an
// *os.PathError because syscall.Open returns a bare Errno: "no such file or
// directory" with no path is useless in a warning about a hierarchy holding
// three files per container.
func openRO(path string) (int, error) {
	for {
		fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return -1, &os.PathError{Op: "open", Path: path, Err: err}
		}
		return fd, nil
	}
}

// readValue preads a whole-file integer (memory.current) from offset 0.
//
// It carries readField's completeness rule, which it used to lack: a single
// pread is allowed to return FEWER bytes than are available, and parseUint
// happily reads a prefix — so "1048576\n" arriving as "104" yielded 104 bytes
// of working set, a number four orders of magnitude wrong and indistinguishable
// from a real reading. A value is accepted only once its line terminator has
// arrived, or once a zero-length read has proved the file ends there.
func readValue(fd int, buf []byte) (uint64, error) {
	n := 0
	for {
		r, err := pread(fd, buf[n:], int64(n))
		if err != nil {
			return 0, err
		}
		if r == 0 {
			break // EOF: what we hold is the whole file
		}
		n += r
		if bytes.IndexByte(buf[:n], '\n') >= 0 {
			break // terminated: the number cannot grow further
		}
		if n == len(buf) {
			return 0, errValueTooLong
		}
	}
	v, ok := parseUint(buf[:n])
	if !ok {
		return 0, errNotANumber
	}
	return v, nil
}

// readField preads fd from offset 0 and returns the integer following key on
// its own line, stopping as soon as the key is found.
//
// The loop re-scans what it has after each chunk rather than tracking a scan
// cursor: the files this reads are one or two chunks long and the second scan
// costs a memchr over a page, whereas a cursor has to reason about a key
// straddling the boundary. A match on a line that is not yet terminated is
// refused until EOF proves it complete, so a truncated read can never parse a
// prefix of a longer field name or of a longer number.
func readField(fd int, buf []byte, key string) (uint64, error) {
	n := 0
	for {
		r, err := pread(fd, buf[n:], int64(n))
		if err != nil {
			return 0, err
		}
		if r == 0 {
			// EOF: the last line may be unterminated, so scan once more
			// allowing it.
			if v, ok := scanField(buf[:n], key, true); ok {
				return v, nil
			}
			return 0, errFieldMissing
		}
		n += r
		if v, ok := scanField(buf[:n], key, false); ok {
			return v, nil
		}
		if n == len(buf) {
			return 0, errFieldTooFar
		}
	}
}

// pread is syscall.Pread with EINTR retried. Allocation-free: the fd is an int
// and the buffer is the caller's.
func pread(fd int, p []byte, off int64) (int, error) {
	for {
		n, err := syscall.Pread(fd, p, off)
		if err == syscall.EINTR {
			continue
		}
		if n < 0 {
			n = 0
		}
		return n, err
	}
}

// scanField finds "<key> <uint>" on a line of b. complete says b is the whole
// file, which is the only condition under which an unterminated final line may
// be read.
func scanField(b []byte, key string, complete bool) (uint64, bool) {
	for len(b) > 0 {
		var line []byte
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			line, b = b[:i], b[i+1:]
		} else {
			if !complete {
				return 0, false
			}
			line, b = b, nil
		}
		// string(line[:len(key)]) == key does not allocate: the compiler
		// recognises the comparison and works on the bytes in place.
		if len(line) <= len(key) || line[len(key)] != ' ' || string(line[:len(key)]) != key {
			continue
		}
		return parseUint(line[len(key)+1:])
	}
	return 0, false
}

// parseUint reads a leading unsigned decimal, ignoring surrounding blanks and
// any trailing bytes. Hand-written rather than strconv.ParseUint because that
// takes a string (a copy per call, on a path that runs three times per
// container per second) and because a cgroup value is only ever this shape.
//
// Overflow is refused rather than wrapped: cpu.stat's usage_usec is monotonic,
// so a wrapped parse would produce a value BELOW the previous one and be read
// as a counter reset — a silent hole in the series instead of a counted error.
func parseUint(b []byte) (uint64, bool) {
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\t') {
		i++
	}
	start := i
	var v uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			break
		}
		d := uint64(c - '0')
		if v > (math.MaxUint64-d)/10 {
			return 0, false
		}
		v = v*10 + d
	}
	if i == start {
		return 0, false
	}
	return v, true
}
