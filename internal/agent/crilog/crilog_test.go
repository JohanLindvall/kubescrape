package crilog

import (
	"strings"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	l, err := Parse([]byte("2026-07-05T10:00:00.123456789Z stdout F hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if l.Stream != "stdout" || l.Partial || string(l.Content) != "hello world" {
		t.Fatalf("line = %+v", l)
	}
	if l.Time.UTC() != time.Date(2026, 7, 5, 10, 0, 0, 123456789, time.UTC) {
		t.Fatalf("time = %v", l.Time)
	}

	l, err = Parse([]byte("2026-07-05T10:00:00Z stderr P partial"))
	if err != nil || !l.Partial || l.Stream != "stderr" {
		t.Fatalf("line = %+v err = %v", l, err)
	}

	// Empty content full line.
	l, err = Parse([]byte("2026-07-05T10:00:00Z stdout F"))
	if err != nil || len(l.Content) != 0 {
		t.Fatalf("line = %+v err = %v", l, err)
	}

	for _, bad := range []string{
		"", "garbage", "2026-07-05T10:00:00Z", "2026-07-05T10:00:00Z stdout",
		"notatime stdout F x", "2026-07-05T10:00:00Z tty F x",
		"2026-07-05T10:00:00Z stdout X x",
	} {
		if _, err := Parse([]byte(bad)); err == nil {
			t.Errorf("Parse(%q) should fail", bad)
		}
	}
}

func TestAssemblerFullLines(t *testing.T) {
	a := NewAssembler(1024)
	l, _ := Parse([]byte("2026-07-05T10:00:00Z stdout F one"))
	e, done := a.Add(l)
	if !done || string(e.Body) != "one" || e.Truncated {
		t.Fatalf("entry = %+v done = %v", e, done)
	}
}

func TestAssemblerPartials(t *testing.T) {
	a := NewAssembler(1024)
	lines := []string{
		"2026-07-05T10:00:00Z stdout P first ",
		"2026-07-05T10:00:01Z stdout P second ",
		"2026-07-05T10:00:02Z stdout F third",
	}
	var got Entry
	var count int
	for _, s := range lines {
		l, err := Parse([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		if e, done := a.Add(l); done {
			got = e
			got.Body = append([]byte(nil), e.Body...)
			count++
		}
	}
	if count != 1 || string(got.Body) != "first second third" {
		t.Fatalf("count=%d body=%q", count, got.Body)
	}
	// The entry carries the timestamp of the first partial.
	if got.Time.UTC().Second() != 0 {
		t.Fatalf("time = %v", got.Time)
	}
}

func TestAssemblerTruncation(t *testing.T) {
	a := NewAssembler(10)
	l1, _ := Parse([]byte("2026-07-05T10:00:00Z stdout P " + strings.Repeat("a", 8)))
	l2, _ := Parse([]byte("2026-07-05T10:00:00Z stdout F " + strings.Repeat("b", 8)))
	if _, done := a.Add(l1); done {
		t.Fatal("partial should not complete")
	}
	e, done := a.Add(l2)
	if !done || !e.Truncated || len(e.Body) != 10 {
		t.Fatalf("entry = %+v done = %v", e, done)
	}
}

func TestAssemblerFlush(t *testing.T) {
	a := NewAssembler(1024)
	l, _ := Parse([]byte("2026-07-05T10:00:00Z stdout P dangling"))
	_, _ = a.Add(l)
	e, ok := a.Flush()
	if !ok || string(e.Body) != "dangling" {
		t.Fatalf("entry = %+v ok = %v", e, ok)
	}
	if _, ok := a.Flush(); ok {
		t.Fatal("second flush should be empty")
	}
}
