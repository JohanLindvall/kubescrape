package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/store"
)

// -max-blocked-lookups exists because the cap it sets is a MEMORY budget
// divided by a MEASURED per-waiter cost, and the memory in that division is the
// 128Mi the DEFAULT chart gives this pod. An operator who gives it more, or runs
// a fleet bigger than the default cap admits, has to be able to say so — the cap
// was previously a compile-time constant with no production caller for the
// setter, so a pod given 2Gi still shed at the small-pod number.
//
// The flag's default must stay the derivation, not a copy of today's value of
// it: a hand-typed default drifts silently the moment either constant moves.
func TestMaxBlockedLookupsDefaultsToTheDerivedCap(t *testing.T) {
	f := flag.CommandLine.Lookup("max-blocked-lookups")
	if f == nil {
		t.Fatal("-max-blocked-lookups is not registered: the documented knob does not exist")
	}
	want := strconv.Itoa(store.DefaultMaxWaiters)
	if f.DefValue != want {
		t.Fatalf("-max-blocked-lookups default = %s, want store.DefaultMaxWaiters = %s "+
			"(WaiterBudgetBytes/WaiterCostBytes); a default that is not the derivation drifts from it", f.DefValue, want)
	}
	if *maxWaiters != store.DefaultMaxWaiters {
		t.Fatalf("parsed value = %d before any -max-blocked-lookups is passed, want the default %d",
			*maxWaiters, store.DefaultMaxWaiters)
	}
}

func TestCheckWaiterCap(t *testing.T) {
	for _, tc := range []struct {
		name    string
		n       int
		wantErr bool
		wantLog bool
	}{
		// 0 and below mean "shed every blocking lookup", which would 503 the
		// first log line of every container on every node.
		{name: "zero", n: 0, wantErr: true},
		{name: "negative", n: -1, wantErr: true},
		{name: "one", n: 1},
		{name: "the default", n: store.DefaultMaxWaiters},
		// Exactly at the budget is still within it — the default IS this value,
		// so warning here would warn on every default start.
		{name: "at the budget", n: store.WaiterBudgetBytes / store.WaiterCostBytes},
		// Past it is allowed and warned: the operator may have given the pod the
		// memory, but the multiplication has to be visible somewhere.
		{name: "over the budget", n: store.WaiterBudgetBytes/store.WaiterCostBytes + 1, wantLog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			err := checkWaiterCap(tc.n, log)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkWaiterCap(%d) error = %v, want error: %v", tc.n, err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "-max-blocked-lookups") {
				t.Fatalf("error %q does not name the flag an operator has to change", err)
			}
			warned := strings.Contains(buf.String(), "above the memory budget")
			if warned != tc.wantLog {
				t.Fatalf("checkWaiterCap(%d) warned = %v, want %v (log: %s)", tc.n, warned, tc.wantLog, buf.String())
			}
			if tc.wantLog && !strings.Contains(buf.String(), "saturatedMiB") {
				t.Fatalf("the warning does not do the multiplication for the operator: %s", buf.String())
			}
		})
	}
}

// …and the flag has to REACH the shed decision. The setter it drives had no
// production caller before this flag existed, so the cap was whatever the
// constant said however much memory the pod had been given; a flag that is
// parsed and then dropped would look exactly the same from outside.
func TestWaiterCapReachesTheShedDecision(t *testing.T) {
	st := newMetadataStore(time.Minute, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// One blocking lookup for an id no upsert will ever supply: it parks.
	go func() { _, _, _ = st.GetContainer(ctx, strings.Repeat("a", 64)) }()
	deadline := time.Now().Add(10 * time.Second)
	for st.BlockedLookups() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("no lookup parked")
		}
		time.Sleep(time.Millisecond)
	}

	// The second one is over the cap of 1, so it must be SHED — retryably, not
	// as a miss: a 404 would tell the agent the container does not exist. The
	// deadline is what turns "the cap was not applied" into a FAILURE rather
	// than a hang: without it this lookup simply parks alongside the first.
	shedCtx, shedCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shedCancel()
	_, ok, err := st.GetContainer(shedCtx, strings.Repeat("b", 64))
	if !errors.Is(err, store.ErrTooManyWaiters) {
		t.Fatalf("second lookup returned (ok=%v, err=%v), want store.ErrTooManyWaiters: "+
			"the cap the flag set is not the cap the store applies", ok, err)
	}
	if st.ShedLookups() != 1 {
		t.Fatalf("ShedLookups = %d, want 1", st.ShedLookups())
	}
}
