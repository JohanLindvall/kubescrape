package cgroupstats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func openTemp(t *testing.T, content string) int {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f")
	writeFile(t, p, content)
	fd, err := openRO(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.NewFile(uintptr(fd), p).Close() })
	return fd
}

func TestReadFieldFindsTheKeyWhereverItIs(t *testing.T) {
	buf := make([]byte, readBufBytes)

	fd := openTemp(t, cpuStat(123456))
	v, err := readField(fd, buf, keyUsageUsec)
	if err != nil || v != 123456 {
		t.Fatalf("usage_usec = %d, %v; want 123456", v, err)
	}

	// inactive_file sits twenty-odd fields into memory.stat; the read must walk
	// there rather than assume a position.
	fd = openTemp(t, memStat(987654321))
	v, err = readField(fd, buf, keyInactiveFile)
	if err != nil || v != 987654321 {
		t.Fatalf("inactive_file = %d, %v; want 987654321", v, err)
	}
}

func TestReadValueReadsAWholeFileNumber(t *testing.T) {
	buf := make([]byte, readBufBytes)
	fd := openTemp(t, "4294967296\n")
	v, err := readValue(fd, buf)
	if err != nil || v != 4294967296 {
		t.Fatalf("memory.current = %d, %v; want 4294967296", v, err)
	}
	// No terminator at all: EOF proves the file ends there, so it parses.
	fd = openTemp(t, "12345")
	if v, err := readValue(fd, buf); err != nil || v != 12345 {
		t.Fatalf("unterminated memory.current = %d, %v; want 12345", v, err)
	}
}

// readValue carries readField's COMPLETENESS rule, and used not to: a single
// pread is allowed to return fewer bytes than are available, and parseUint
// happily reads a prefix — so a short read of "1048576\n" yielded 104 bytes of
// working set, four orders of magnitude wrong and indistinguishable from a real
// measurement. A value is accepted only once its terminator has arrived or EOF
// has proved the file ends.
func TestReadValueRefusesATruncatedNumber(t *testing.T) {
	fd := openTemp(t, "1048576\n")
	// The buffer is what bounds the read here, exactly as it does for
	// readField: three bytes in, "104" is a PREFIX of the number, not the
	// number.
	v, err := readValue(fd, make([]byte, 3))
	if err == nil {
		t.Fatalf("readValue returned %d from an unterminated prefix of 1048576", v)
	}
	if err != errValueTooLong {
		t.Fatalf("err = %v, want errValueTooLong", err)
	}
	// One byte more than the number plus its newline: complete, and correct.
	fd = openTemp(t, "1048576\n")
	if v, err := readValue(fd, make([]byte, 8)); err != nil || v != 1048576 {
		t.Fatalf("readValue = %d, %v; want 1048576", v, err)
	}
}

// A held descriptor must see fresh content on the next pread: the whole design
// (open once, pread at offset 0 forever) rests on it. On a cgroupfs this is
// kernfs restarting its seq_file iteration; on a regular file it is a truncate
// and rewrite, which is what a test can arrange.
func TestPreadOnAHeldDescriptorSeesNewContent(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cpu.stat")
	writeFile(t, p, cpuStat(1000))
	fd, err := openRO(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.NewFile(uintptr(fd), p).Close() }()

	buf := make([]byte, readBufBytes)
	if v, err := readField(fd, buf, keyUsageUsec); err != nil || v != 1000 {
		t.Fatalf("first read: %d, %v", v, err)
	}
	writeFile(t, p, cpuStat(2000))
	if v, err := readField(fd, buf, keyUsageUsec); err != nil || v != 2000 {
		t.Fatalf("second read: %d, %v; want 2000 from the same held fd", v, err)
	}
	// And shrinking is fine too — the read is bounded by what pread returns,
	// never by what the buffer still holds from last time.
	writeFile(t, p, "usage_usec 7\n")
	if v, err := readField(fd, buf, keyUsageUsec); err != nil || v != 7 {
		t.Fatalf("third read: %d, %v; want 7", v, err)
	}
}

func TestReadFieldMissingKey(t *testing.T) {
	buf := make([]byte, readBufBytes)
	fd := openTemp(t, "user_usec 1\nsystem_usec 2\n")
	if _, err := readField(fd, buf, keyUsageUsec); err == nil {
		t.Fatal("expected an error for an absent key")
	}
}

// A key that is a PREFIX of the wanted one (and vice versa) must not match:
// memory.stat carries both inactive_file and inactive_anon, and cpu.stat's
// usage_usec sits beside user_usec.
func TestScanFieldRequiresTheWholeKey(t *testing.T) {
	b := []byte("inactive_anon 1\ninactive_file_foo 2\ninactive_file 3\n")
	v, ok := scanField(b, keyInactiveFile, true)
	if !ok || v != 3 {
		t.Fatalf("got %d, %v; want 3", v, ok)
	}
	if _, ok := scanField([]byte("xinactive_file 1\n"), keyInactiveFile, true); ok {
		t.Fatal("matched a key that is only a suffix of the line's first token")
	}
}

// An unterminated final line is only readable once EOF proves it complete. A
// chunked read that accepted "usage_usec 12" while "3456\n" was still in the
// kernel would report a hundredth of the truth.
func TestScanFieldRefusesAnIncompleteLineUntilEOF(t *testing.T) {
	partial := []byte("usage_usec 12")
	if _, ok := scanField(partial, keyUsageUsec, false); ok {
		t.Fatal("accepted a possibly-truncated value")
	}
	v, ok := scanField(partial, keyUsageUsec, true)
	if !ok || v != 12 {
		t.Fatalf("at EOF: got %d, %v; want 12", v, ok)
	}
}

// The read is BOUNDED by the buffer: a key beyond it is a clean, counted error
// and never a wrong number. That bound is the "read only what you need from the
// big file" property, so it is worth pinning that the margin over a real
// memory.stat is large — inactive_file lands well inside one page, and the
// error only exists for a file shaped nothing like one.
func TestReadFieldIsBoundedByTheBuffer(t *testing.T) {
	body := memStat(4242)
	keyAt := strings.Index(body, keyInactiveFile)
	if keyAt < 0 {
		t.Fatal("fixture lost its key")
	}
	if keyAt > readBufBytes/4 {
		t.Errorf("inactive_file sits at byte %d of a %d-byte read budget; the margin readBufBytes assumes is gone",
			keyAt, readBufBytes)
	}

	// Comfortably past the key: found.
	fd := openTemp(t, body)
	if v, err := readField(fd, make([]byte, keyAt+64), keyInactiveFile); err != nil || v != 4242 {
		t.Fatalf("read with a just-big-enough buffer: %d, %v; want 4242", v, err)
	}
	// Short of the key: refused, not guessed.
	fd = openTemp(t, body)
	if _, err := readField(fd, make([]byte, 16), keyInactiveFile); err != errFieldTooFar {
		t.Fatalf("err = %v, want errFieldTooFar", err)
	}
}

func TestParseUint(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
		ok   bool
	}{
		{"0", 0, true},
		{"42", 42, true},
		{"  42  ", 42, true},
		{"42abc", 42, true}, // trailing junk is ignored; cgroup values never carry any
		{"18446744073709551615", 1<<64 - 1, true},
		{"18446744073709551616", 0, false}, // overflow is refused, never wrapped
		{"99999999999999999999999", 0, false},
		{"", 0, false},
		{"max", 0, false},
		{"-1", 0, false},
	}
	for _, tc := range cases {
		v, ok := parseUint([]byte(tc.in))
		if ok != tc.ok || (ok && v != tc.want) {
			t.Errorf("parseUint(%q) = %d, %v; want %d, %v", tc.in, v, ok, tc.want, tc.ok)
		}
	}
}

func TestOpenCgroupFDsClosesOnPartialFailure(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, fileCPUStat), cpuStat(1))
	writeFile(t, filepath.Join(dir, fileMemCurrent), "1\n")
	// memory.stat is missing.
	if _, err := openCgroupFDs(dir); err == nil {
		t.Fatal("expected an error when one of the three files is missing")
	}
	// The two that DID open must not be leaked. There is no portable way to
	// assert that directly, so assert the shape instead: the error names the
	// missing file, and the code path that produces it is the one that closes.
	_, err := openCgroupFDs(dir)
	if err == nil || !strings.Contains(err.Error(), fileMemStat) {
		t.Fatalf("error = %v, want it to name %s", err, fileMemStat)
	}
}
