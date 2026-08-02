package config

import (
	"strings"
	"testing"
	"time"
)

func TestDurationDefaultsAndParses(t *testing.T) {
	got, err := Duration("x.y", "", 5*time.Second)
	if err != nil || got != 5*time.Second {
		t.Fatalf("empty = %v, %v; want the default", got, err)
	}
	if got, err = Duration("x.y", "250ms", time.Hour); err != nil || got != 250*time.Millisecond {
		t.Fatalf("250ms = %v, %v", got, err)
	}
	// An explicit zero is legal by default and is NOT replaced by the default:
	// "off" must be expressible where the field's default is not zero.
	if got, err = Duration("x.y", "0", time.Hour); err != nil || got != 0 {
		t.Fatalf("explicit 0 = %v, %v; want 0", got, err)
	}
	if got, err = Duration("x.y", "0s", time.Hour, ZeroDisables()); err != nil || got != 0 {
		t.Fatalf("explicit 0s = %v, %v; want 0", got, err)
	}
}

// THE DRIFT this helper resolves: the eight hand-written parsers disagreed
// about negatives. Some errored, one accepted the value and left a later bound
// check to produce an unrelated message, and one accepted it outright — so
// `decisionWait: -5s` and `ignoreOlder: -5s` were a startup failure and a
// silently broken feature depending only on which parser the field happened to
// reach. There is now one answer, and it names the field and the value.
func TestDurationRejectsNegatives(t *testing.T) {
	for _, v := range []string{"-1s", "-100ms", "-1h30m"} {
		_, err := Duration("tailSampling.decisionWait", v, time.Minute)
		if err == nil {
			t.Fatalf("%q accepted", v)
		}
		if !strings.Contains(err.Error(), "tailSampling.decisionWait") || !strings.Contains(err.Error(), v) {
			t.Errorf("error for %q must name the field and the value: %v", v, err)
		}
	}
}

func TestDurationParseErrorNamesFieldAndValue(t *testing.T) {
	_, err := Duration("logs.ignoreOlder", "2quarters", 0)
	if err == nil {
		t.Fatal("a malformed duration must be rejected")
	}
	if !strings.Contains(err.Error(), "logs.ignoreOlder") || !strings.Contains(err.Error(), "2quarters") {
		t.Errorf("error must name the field and the value: %v", err)
	}
}

// Positive folds a caller's follow-up bound check into the same error, so the
// explanation ("a zero window decides every trace on its first span") survives
// the consolidation instead of degrading to a bare "is negative".
func TestDurationPositive(t *testing.T) {
	why := "a zero window decides every trace on its first span"
	_, err := Duration("tailSampling.decisionWait", "0s", 5*time.Second, Positive(why))
	if err == nil {
		t.Fatal("an explicit zero must be rejected for a Positive field")
	}
	if !strings.Contains(err.Error(), why) || !strings.Contains(err.Error(), "tailSampling.decisionWait") {
		t.Errorf("error must name the field and say why: %v", err)
	}
	if _, err := Duration("tailSampling.decisionWait", "", 5*time.Second, Positive(why)); err != nil {
		t.Errorf("a positive default must pass: %v", err)
	}
	if got, err := Duration("tailSampling.decisionWait", "1ns", time.Second, Positive(why)); err != nil || got != time.Nanosecond {
		t.Errorf("1ns = %v, %v; want it accepted", got, err)
	}
}
