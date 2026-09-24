package scrape

// merge_guard_test.go holds MergeMonitorEndpoint's hand-written field lists —
// the bare gate and the auth/TLS adopt — to the one classification in
// merge.go (endpointMergeClass). TestEveryEndpointFieldReachesTheTarget forces
// a new Endpoint field into stampEndpoint; nothing but this file forces it
// into the merge, and a mergeable field the gate cannot see is silently
// dropped on every URL two monitors share.

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Every servicemonitors.Endpoint field must be classified — in exactly one
// class, by map construction — and every classified name must still be a
// field, so a rename cannot leave a stale entry vouching for nothing.
func TestEveryEndpointFieldIsClassifiedForTheMerge(t *testing.T) {
	typ := reflect.TypeFor[servicemonitors.Endpoint]()
	fields := map[string]bool{}
	for field := range typ.Fields() {
		name := field.Name
		fields[name] = true
		if _, ok := endpointMergeClass[name]; !ok {
			t.Errorf("Endpoint.%s is not classified in merge.go's endpointMergeClass: add it there — "+
				"inertClass only if two endpoints resolving to ONE URL cannot meaningfully differ on it; "+
				"a mergeable class also needs the bare gate in MergeMonitorEndpoint extended, and auth "+
				"material belongs in kubemeta.ScrapeAuth, which the merge compares and adopts whole", name)
		}
	}
	for name := range endpointMergeClass {
		if !fields[name] {
			t.Errorf("endpointMergeClass names %q, which is not a servicemonitors.Endpoint field (renamed or removed?)", name)
		}
	}
}

// classifiedLeaves is endpointMergeClass with the embedded auth/TLS group
// expanded into its own fields: the group is ONE entry in the classification
// (it is compared and assigned whole), but the guards below exercise every
// field of it individually, so a field the struct compare somehow could not
// see would still fail here.
func classifiedLeaves(t *testing.T) map[string]mergeClass {
	t.Helper()
	out := map[string]mergeClass{}
	for name, class := range endpointMergeClass {
		if name != "ScrapeAuth" {
			out[name] = class
			continue
		}
		if class != authClass {
			t.Fatalf("the embedded ScrapeAuth group is classified %v, want authClass", class)
		}
		for _, f := range reflect.VisibleFields(reflect.TypeFor[kubemeta.ScrapeAuth]()) {
			out[f.Name] = authClass
		}
	}
	if _, ok := out["AuthCredentials"]; !ok {
		t.Fatal("the auth/TLS group was not expanded: endpointMergeClass has no ScrapeAuth entry")
	}
	return out
}

// endpointCarryingOnly fabricates an endpoint whose ONLY set field is the
// named one — the shape a merge-gate omission turns into silent loss.
func endpointCarryingOnly(t *testing.T, field string) *servicemonitors.Endpoint {
	t.Helper()
	ep := &servicemonitors.Endpoint{}
	setEndpointField(t, ep, field, "sentinel-"+field)
	return ep
}

// setEndpointField handles the kinds Endpoint carries today; a field of a new
// kind must extend this rather than dodge the guard.
func setEndpointField(t *testing.T, ep *servicemonitors.Endpoint, field, sentinel string) {
	t.Helper()
	v := reflect.ValueOf(ep).Elem().FieldByName(field)
	if !v.IsValid() {
		t.Fatalf("Endpoint has no field %q", field)
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(sentinel)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
	default:
		t.Fatalf("Endpoint.%s has kind %s this guard cannot fabricate a value for: extend setEndpointField", field, v.Kind())
	}
}

// targetCarriesString walks every string the target carries, the PROMOTED
// auth/TLS fields included — a top-level walk would stop at the embedded
// struct and report a carried sentinel as lost.
func targetCarriesString(tgt kubemeta.ScrapeTarget, sentinel string) bool {
	v := reflect.ValueOf(tgt)
	for _, sf := range reflect.VisibleFields(v.Type()) {
		if sf.Anonymous {
			continue
		}
		if f := v.FieldByIndex(sf.Index); f.Kind() == reflect.String && strings.Contains(f.String(), sentinel) {
			return true
		}
	}
	return false
}

// For each classified field, an endpoint carrying ONLY that field must get
// what its class declares: mergeable fields pass the bare gate, contribute,
// and land on the target (auth adopted whole, one-sided); inert fields read as
// bare and leave the holder untouched. A mergeable field the gate misses
// fails HERE, not as up=0 in production.
func TestMergeHonoursEveryClassifiedField(t *testing.T) {
	for field, class := range classifiedLeaves(t) {
		t.Run(field, func(t *testing.T) {
			ep := endpointCarryingOnly(t, field)
			held := mergeHeld("ns/a", servicemonitors.Endpoint{})
			if class == inertClass {
				want := held
				rep := MergeMonitorEndpoint(&held, "ns/b", ep)
				adopted, conflict := rep.AuthAdopted, rep.AuthConflict
				if adopted || conflict || held.Monitors != nil || !reflect.DeepEqual(held, want) {
					t.Fatalf("Endpoint.%s is classified inert but the merge acted on it: adopted=%v conflict=%v target=%+v",
						field, adopted, conflict, held)
				}
				return
			}
			if class == authClass && ep.ScrapeAuth == (kubemeta.ScrapeAuth{}) {
				t.Fatalf("Endpoint.%s is classified auth material but is not part of the embedded "+
					"kubemeta.ScrapeAuth: an endpoint carrying only it reads as BARE and the declaration "+
					"is dropped on any shared URL", field)
			}
			rep := MergeMonitorEndpoint(&held, "ns/b", ep)
			adopted, conflict := rep.AuthAdopted, rep.AuthConflict
			if conflict {
				t.Fatalf("Endpoint.%s carried alone conflicted against an empty holder", field)
			}
			if want := []string{"ns/a", "ns/b"}; !slices.Equal(held.Monitors, want) {
				t.Fatalf("Endpoint.%s is classified mergeable but did not contribute (monitors %v): the bare gate "+
					"in MergeMonitorEndpoint cannot see it — add an arm beside the other mergeable classes", field, held.Monitors)
			}
			switch class {
			case relabelClass:
				if len(held.MetricRelabelings) == 0 {
					t.Fatalf("Endpoint.%s did not land on the target's relabel chain", field)
				}
			case cadenceClass:
				if !targetCarriesString(held, "sentinel-"+field) {
					t.Fatalf("Endpoint.%s did not land on the target: %+v", field, held)
				}
			case authClass:
				if !adopted {
					t.Fatalf("Endpoint.%s: one-sided auth material was not reported adopted", field)
				}
				if got, want := held.ScrapeAuth, ep.ScrapeAuth; got != want {
					t.Fatalf("Endpoint.%s was lost in the adopt: got %+v want %+v", field, got, want)
				}
			}
		})
	}
}

// A holder and an endpoint differing in any SINGLE auth/TLS field must
// conflict with the holder's group kept whole — never a re-adoption, and never
// a mix of the two monitors' material.
func TestEachDeclaredAuthFieldConflictsWhenDiffering(t *testing.T) {
	for _, f := range reflect.VisibleFields(reflect.TypeFor[kubemeta.ScrapeAuth]()) {
		field := f.Name
		if f.Type.Kind() != reflect.String {
			// A zero bool cannot express a second, differing one-field
			// endpoint; InsecureSkipVerify's conflict arm is pinned in
			// TestMergeAuthIdenticalKeepsConflictingReports.
			continue
		}
		t.Run(field, func(t *testing.T) {
			ep := endpointCarryingOnly(t, field)
			held := mergeHeld("ns/a", *ep)
			ep2 := &servicemonitors.Endpoint{}
			setEndpointField(t, ep2, field, "sentinel-2-"+field)
			rep := MergeMonitorEndpoint(&held, "ns/b", ep2)
			adopted, conflict := rep.AuthAdopted, rep.AuthConflict
			if adopted || !conflict {
				t.Fatalf("Endpoint.%s differing: adopted=%v conflict=%v, want the conflict reported", field, adopted, conflict)
			}
			if got, want := held.ScrapeAuth, ep.ScrapeAuth; got != want {
				t.Fatalf("Endpoint.%s: the holder's auth group was not kept whole: got %+v want %+v", field, got, want)
			}
		})
	}
}
