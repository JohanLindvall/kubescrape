package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// The blocked-lookup cap is a MEMORY budget expressed in waiters, and what THIS
// package can check about it is that the number the budget derives is the number
// the store APPLIES — through the real admission path, at the real default, in
// both directions: the derived number of lookups must all park, and the next one
// must be shed retryably.
//
// What it deliberately does NOT do is assert the derivation against itself.
// Its predecessor did, three times over: `DefaultMaxWaiters ==
// WaiterBudgetBytes/WaiterCostBytes` is how the constant is DECLARED, and both
// "n x cost fits the budget" and "(n+1) x cost does not" follow from integer
// division over an exact multiple. None of the three could fail for any edit to
// either constant, including the "not hand-placed under its own derivation"
// direction they were written to cover — which the loop below covers for real,
// because a cap one below the derivation leaves the last lookup shed instead of
// parked.
//
// The per-waiter COST is measured elsewhere and has to be: none of it is
// allocated in this package (it is a parked net/http handler). See
// internal/server's TestParkedLookupCostFitsItsBudget.
func TestTheDefaultCapIsTheCapTheStoreApplies(t *testing.T) {
	s := New(time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Distinct, well-formed IDs no upsert will ever supply: each parks on its
	// own waiter, and cancelling the context at the end releases them all.
	for i := range DefaultMaxWaiters {
		go func() { _, _, _ = s.GetContainer(ctx, fmt.Sprintf("%064x", i)) }()
	}
	deadline := time.Now().Add(30 * time.Second)
	for s.BlockedLookups() < DefaultMaxWaiters {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of the derived %d lookups parked: the store sheds BELOW its own default, "+
				"so the cap leaves memory the budget allowed unspent and reports ordinary demand as abuse "+
				"on kubescrape_container_lookups_shed_total", s.BlockedLookups(), DefaultMaxWaiters)
		}
		time.Sleep(time.Millisecond)
	}

	// …and the next one over is shed, retryably. The deadline is what turns "the
	// default was never applied" into a failure rather than a hang: without a
	// cap this lookup simply parks alongside the others.
	shedCtx, shedCancel := context.WithTimeout(ctx, 10*time.Second)
	defer shedCancel()
	before := s.ShedLookups()
	_, ok, err := s.GetContainer(shedCtx, fmt.Sprintf("%064x", DefaultMaxWaiters))
	if !errors.Is(err, ErrTooManyWaiters) {
		t.Fatalf("lookup %d returned (ok=%v, err=%v), want ErrTooManyWaiters: the default cap is not the "+
			"cap the store applies, so DefaultMaxWaiters bounds nothing", DefaultMaxWaiters+1, ok, err)
	}
	if got := s.ShedLookups() - before; got != 1 {
		t.Fatalf("ShedLookups moved by %d, want 1: the refusal is the operator's only signal that the cap bound", got)
	}
}

// A container lookup parks in TWO places — the waiter here and, while the
// caches are still syncing, internal/server's readiness wait — and they cost
// the same parked handler on the same unauthenticated route. So they spend ONE
// budget, in BOTH directions: a slot taken by TryPark denies a waiter, and a
// waiter denies a TryPark. Either half alone would let the documented cap be
// exceeded by exactly the number of lookups sitting in the other spot.
func TestTheTwoParkingSpotsShareOneBudget(t *testing.T) {
	t.Run("an external park denies a waiter", func(t *testing.T) {
		s := New(time.Minute)
		s.SetMaxWaiters(2)
		for i := range 2 {
			if !s.TryPark() {
				t.Fatalf("external park %d refused below the cap of 2", i)
			}
		}
		if got := s.BlockedLookups(); got != 2 {
			t.Fatalf("BlockedLookups = %d with two external parks held, want 2: the gauge is blind to "+
				"the spot that is full", got)
		}
		// The budget is spent, so the store's own waiter is refused — retryably,
		// never as a miss. The timeout is what turns "the external parks were
		// not counted" into a failure rather than a five-second park.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		before := s.ShedLookups()
		_, ok, err := s.GetContainer(ctx, fmt.Sprintf("%064x", 1))
		if !errors.Is(err, ErrTooManyWaiters) {
			t.Fatalf("GetContainer returned (ok=%v, err=%v) while the cap of 2 was spent by external parks, "+
				"want ErrTooManyWaiters: the two spots have a budget each, so the cap bounds neither", ok, err)
		}
		if got := s.ShedLookups() - before; got != 1 {
			t.Fatalf("ShedLookups moved by %d, want 1", got)
		}
		// …and a returned slot is usable again.
		s.Unpark()
		ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel2()
		if _, _, err := s.GetContainer(ctx2, fmt.Sprintf("%064x", 2)); err != nil {
			t.Fatalf("GetContainer after Unpark returned %v, want it to park and then miss: the slot was "+
				"never returned", err)
		}
	})

	t.Run("a waiter denies an external park", func(t *testing.T) {
		s := New(time.Minute)
		s.SetMaxWaiters(1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { _, _, _ = s.GetContainer(ctx, fmt.Sprintf("%064x", 1)) }()
		deadline := time.Now().Add(10 * time.Second)
		for s.BlockedLookups() < 1 {
			if time.Now().After(deadline) {
				t.Fatal("no lookup parked")
			}
			time.Sleep(time.Millisecond)
		}
		before := s.ShedLookups()
		if s.TryPark() {
			t.Fatal("an external park was admitted while the store's own waiter had already spent the " +
				"cap of 1: -max-blocked-lookups bounds each spot separately, so a fleet parked on the " +
				"readiness gate holds the cap's worth of memory TWICE")
		}
		if got := s.ShedLookups() - before; got != 1 {
			t.Fatalf("ShedLookups moved by %d, want 1: a refusal an operator cannot see", got)
		}
	})
}

// The cap is settable, and a Store built by New starts at the default. This is
// the wiring -max-blocked-lookups rides on; without it the documented knob would
// be a flag that changes nothing.
func TestNewStoreStartsAtTheDefaultCapAndSetOverridesIt(t *testing.T) {
	s := New(0)
	if s.maxWaiters != DefaultMaxWaiters {
		t.Fatalf("New store maxWaiters = %d, want DefaultMaxWaiters = %d", s.maxWaiters, DefaultMaxWaiters)
	}
	s.SetMaxWaiters(DefaultMaxWaiters * 3)
	if s.maxWaiters != DefaultMaxWaiters*3 {
		t.Fatalf("after SetMaxWaiters, maxWaiters = %d, want %d", s.maxWaiters, DefaultMaxWaiters*3)
	}
}
